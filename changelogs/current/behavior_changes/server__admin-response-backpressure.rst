Chunked admin responses pause production when downstream write buffers are backed up and resume
when they drain. Disconnected streams cancel pending production and release request state.
Buffered handlers still construct their complete response before sending it.
