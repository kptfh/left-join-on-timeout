# left-join-on-timeout
Kafka Streams left join on timeout

## Usage
Eliminates the lack of sql-like left join semantic in kafka streams framework.
This implementation will generate left join event 
only if full join event didn't happen in join window duration interval.
The main idea behind the scene is to schedule left joined event on left stream and cancel it on full joined stream. 