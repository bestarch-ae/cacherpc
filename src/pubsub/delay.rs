use std::collections::HashMap;
use std::time::Duration;

use actix::prelude::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::time::DelayQueue;

use crate::metrics::pubsub_metrics as metrics;

pub(crate) enum DelayQueueCommand<T> {
    Insert(T, Instant),
    Reset(T, Instant),
}

pub(super) struct DelayQueueHandle<T>(mpsc::UnboundedSender<DelayQueueCommand<T>>);

impl<T> DelayQueueHandle<T> {
    fn insert_at(&self, item: T, time: Instant) {
        let _ = self.0.send(DelayQueueCommand::Insert(item, time));
    }

    pub(super) fn insert(&self, item: T, dur: Duration) {
        self.insert_at(item, Instant::now() + dur)
    }

    pub(super) fn reset(&self, item: T, dur: Duration) {
        let _ = self
            .0
            .send(DelayQueueCommand::Reset(item, Instant::now() + dur));
    }
}

pub(super) fn delay_queue<T: Clone + std::hash::Hash + Eq>(
    id: String,
) -> (DelayQueueHandle<T>, impl Stream<Item = T>) {
    let (sender, incoming) = mpsc::unbounded_channel::<DelayQueueCommand<T>>();
    let mut map: HashMap<T, _> = HashMap::default();
    let stream = stream_generator::generate_stream(|mut stream| async move {
        let mut delay_queue = DelayQueue::new();
        tokio::pin!(incoming);

        loop {
            metrics()
                .purge_queue_length
                .with_label_values(&[&id])
                .set(delay_queue.len() as i64);
            metrics()
                .purge_queue_entries
                .with_label_values(&[&id])
                .set(map.len() as i64);
            tokio::select! {
                item = incoming.recv() => {
                    if let Some(item) = item {
                        match item {
                            DelayQueueCommand::Insert(item, time) | DelayQueueCommand::Reset(item, time) => {
                                if let Some(key) = map.get(&item) {
                                    delay_queue.reset_at(key, time);
                                } else {
                                    map.insert(item.clone(), delay_queue.insert_at(item, time));
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                out = delay_queue.next(), if !delay_queue.is_empty() => {
                    if let Some(Ok(out)) = out {
                        let item = out.into_inner();
                        map.remove(&item);
                        stream.send(item).await;
                    }
                }
            }
        }
    });
    (DelayQueueHandle(sender), stream)
}
