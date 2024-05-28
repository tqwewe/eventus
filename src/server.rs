use futures::stream::BoxStream;
use futures::StreamExt;
use kameo::actor::{Actor, ActorRef};
use kameo::error::{BoxError, SendError};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tonic::{transport::Server, Request, Response, Status};

use eventstore::event_store_server::{EventStore, EventStoreServer};
use eventstore::{
    AcknowledgeRequest, Any, AppendEventsRequest, Event, Exact, NewEvent, NoStream, StreamExists,
    SubscribeRequest,
};

use crate::actor::{AppendToStream, LoadSubscription, ReadBatch, Subscribe, UpdateSubscription};
use crate::{subscription_store, AppendError, CommitLog, ReadLimit};

use self::eventstore::EventBatch;

pub mod eventstore {
    use std::{borrow::Cow, time::UNIX_EPOCH};

    use chrono::{DateTime, Utc};
    use prost_types::Timestamp;

    use self::expected_version::Version;

    tonic::include_proto!("eventstore");

    pub struct InvalidTimestamp;

    impl From<Option<ExpectedVersion>> for crate::ExpectedVersion {
        fn from(v: Option<ExpectedVersion>) -> Self {
            match v {
                Some(ExpectedVersion {
                    version: Some(version),
                }) => match version {
                    Version::Any(_) => crate::ExpectedVersion::Any,
                    Version::StreamExists(_) => crate::ExpectedVersion::StreamExists,
                    Version::NoStream(_) => crate::ExpectedVersion::NoStream,
                    Version::Exact(Exact { value }) => crate::ExpectedVersion::Exact(value),
                },
                Some(ExpectedVersion { version: None }) | None => crate::ExpectedVersion::Any,
            }
        }
    }

    impl TryFrom<crate::Event<'static>> for Event {
        type Error = InvalidTimestamp;

        fn try_from(event: crate::Event<'static>) -> Result<Self, Self::Error> {
            let epoch = DateTime::<Utc>::from(UNIX_EPOCH);
            let duration_since_epoch = event.timestamp.signed_duration_since(epoch);
            let timestamp = duration_since_epoch
                .to_std()
                .map_err(|_| InvalidTimestamp)?;
            Ok(Event {
                id: event.id,
                stream_id: event.stream_id.into_owned(),
                stream_version: event.stream_version,
                event_name: event.event_name.into_owned(),
                event_data: event.event_data.into_owned(),
                metadata: event.metadata.into_owned(),
                timestamp: Some(Timestamp {
                    seconds: timestamp
                        .as_secs()
                        .try_into()
                        .map_err(|_| InvalidTimestamp)?,
                    nanos: timestamp
                        .subsec_nanos()
                        .try_into()
                        .map_err(|_| InvalidTimestamp)?,
                }),
            })
        }
    }

    impl TryFrom<Event> for crate::Event<'static> {
        type Error = InvalidTimestamp;

        fn try_from(event: Event) -> Result<Self, Self::Error> {
            Ok(crate::Event {
                id: event.id,
                stream_id: Cow::Owned(event.stream_id),
                stream_version: event.stream_version,
                event_name: Cow::Owned(event.event_name),
                event_data: Cow::Owned(event.event_data),
                metadata: Cow::Owned(event.metadata),
                timestamp: event
                    .timestamp
                    .and_then(|ts| {
                        Some(
                            DateTime::<Utc>::from(UNIX_EPOCH)
                                + chrono::Duration::from_std(std::time::Duration::new(
                                    ts.seconds.try_into().ok()?,
                                    ts.nanos.try_into().ok()?,
                                ))
                                .ok()?,
                        )
                    })
                    .ok_or(InvalidTimestamp)?,
            })
        }
    }

    impl From<NewEvent> for crate::NewEvent<'static> {
        fn from(event: NewEvent) -> Self {
            crate::NewEvent {
                event_name: Cow::Owned(event.event_name),
                event_data: Cow::Owned(event.event_data),
                metadata: Cow::Owned(event.metadata),
            }
        }
    }
}

#[derive(Debug)]
pub struct DefaultEventStoreServer {
    log: ActorRef<CommitLog>,
}

#[tonic::async_trait]
impl EventStore for DefaultEventStoreServer {
    type SubscribeStream = BoxStream<'static, Result<EventBatch, Status>>;

    async fn append_events(
        &self,
        request: Request<AppendEventsRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let res = self
            .log
            .send(AppendToStream {
                stream_id: req.stream_id,
                expected_version: req.expected_version.into(),
                events: req.events.into_iter().map(|event| event.into()).collect(),
            })
            .await;
        match res {
            Ok(_) => Ok(Response::new(())),
            Err(SendError::HandlerError(AppendError::MessageSizeExceeded)) => {
                Err(Status::failed_precondition("message size exceeded"))
            }
            Err(SendError::HandlerError(err @ AppendError::WrongExpectedVersion { .. })) => {
                Err(Status::failed_precondition(err.to_string()))
            }
            Err(err) => Err(Status::internal(err.to_string())),
        }
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let (tx, mut rx) = mpsc::channel(64);

        let mut start_offset = self
            .log
            .send(LoadSubscription {
                id: req.subscriber_id,
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?
            .unwrap_or(0);

        let log = self.log.clone();
        tokio::spawn(async move {
            // Subscribe, and save to a buffer
            let (oneshot_tx, oneshot_rx) = oneshot::channel();
            tokio::spawn({
                let log = log.clone();
                async move {
                    let buffer = Vec::new();

                    let mut subscription = log.send(Subscribe {}).await.map_err_internal()?;

                    // Consume into buffer whilst serving historical events
                    let tx: Sender<Vec<crate::Event<'static>>>;
                    let last_offset: u64;
                    loop {
                        tokio::select! {
                            res = subscription.recv() => {
                                buffer.push(res.map_err_internal()?);
                            }
                            res = oneshot_rx => {
                                (tx, last_offset) = res.map_err_internal()?;
                                break;
                            }
                        }
                    }

                    // Serve buffer
                    for batch in buffer {
                        batch.retain(|event| event.id > last_offset);
                        tx.send(batch).await.map_err_internal()?;
                    }

                    // Serve from subscription
                    while let Ok(batch) = subscription.recv().await {
                        tx.send(batch).await.map_err_internal()?;
                    }

                    Ok::<_, Status>(buffer)
                }
            });

            // Start streaming from history
            loop {
                let Ok(batch) = log
                    .send(ReadBatch {
                        start_offset,
                        read_limit: ReadLimit(10_240),
                    })
                    .await
                else {
                    break;
                };

                if batch.is_empty() {
                    break;
                }

                start_offset = batch.last().map(|ev| ev.id).unwrap_or(start_offset);

                if tx.send(batch).await.is_err() {
                    break;
                }
            }

            // Stop saving subscription to buffer
            oneshot_tx.send((tx, start_offset));

            // Serve buffer
            // Serve from subscription
        });

        let stream = ReceiverStream::new(rx).map(|events| {
            let events = events
                .into_iter()
                .map(Event::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| Status::internal("invalid timestamp"))?;
            Ok(EventBatch { events })
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn acknowledge(
        &self,
        request: Request<AcknowledgeRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        self.log
            .send(UpdateSubscription {
                id: req.subscriber_id,
                last_event_id: req.last_event_id,
            })
            .await
            .map_err(|err| Status::internal(err.to_string()))?;

        Ok(Response::new(()))
    }
}

trait MapInternalError<T> {
    fn map_err_internal(self) -> Result<T, Status>;
}

impl<T, E> MapInternalError<T> for Result<T, E>
where
    E: ToString,
{
    fn map_err_internal(self) -> Result<T, Status> {
        self.map_err(|err| Status::internal(err.to_string()))
    }
}
