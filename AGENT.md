Implement the task described in task.txt.

This is a small Go test assignment. The priority is correctness, simplicity, and readability. Do not over-engineer.

Required behavior

Before making changes:

Read task.txt fully.
Extract the exact functional requirements.
Implement only what is required by the task.

Do not add features that are not requested.

Implementation constraints
Use Go.
Use only the Go standard library.
Prefer a single main.go file.
Do not create a classic project layout like cmd/, internal/, pkg/, etc.
Do not add third-party dependencies.
Do not add Docker, Makefile, README, config files, CI, linters, or extra tooling unless explicitly requested.
Keep the code compact and understandable.
Prefer less code when it does not harm correctness.
Do not add debug logs.
Do not print normal runtime logs.
Only print startup/argument/listen errors if needed.
Architecture expectations

The implementation should solve the queue broker task directly.

A good model is:

one in-memory broker;
multiple named queues;
each queue stores:
ready messages in FIFO order;
waiting GET requests in FIFO order.

Correctness is more important than cleverness.

The implementation must handle concurrent HTTP requests safely.

Use synchronization from the standard library, for example sync.Mutex.

Important correctness requirements

Pay special attention to these cases:

PUT /queue?v=message
returns 200 OK with an empty body;
returns 400 Bad Request if query parameter v is missing;
queue name is taken from the URL path.
GET /queue
returns the oldest queued message;
returns 404 Not Found if the queue is empty and no timeout is provided.
GET /queue?timeout=N
waits up to N seconds for a message;
returns the message if it appears before timeout;
returns 404 Not Found if no message appears before timeout.
FIFO message order must be preserved.
FIFO waiter order must be preserved:
if two GET requests are waiting for the same queue,
the first arriving message must go to the first waiting GET request.
Do not lose messages on races between:
timeout expiration;
client cancellation;
concurrent PUT request.
Do not deliver one message to multiple clients.
Do not leave canceled or timed-out waiters permanently in memory.
HTTP behavior
The port must be passed through command-line arguments.
Accept both 8080 and :8080 as reasonable port argument forms if convenient.
For unsupported methods, return 405 Method Not Allowed.
For invalid timeout values, return 400 Bad Request.
Empty message value is allowed if the parameter v is present.
Example: PUT /q?v= should be accepted.
Missing v should be rejected.
Code style
Write plain, idiomatic Go.
Use descriptive names.
Avoid abstractions that do not reduce complexity.
Avoid generic interfaces, dependency injection, background workers, persistence, metrics, graceful shutdown machinery, or configuration layers.
Keep the solution easy to review in one pass.
Run gofmt.
Testing / verification

After implementation, verify manually with curl or a small temporary local script.

Required checks:

Basic FIFO:
curl -i -XPUT 'http://127.0.0.1:8080/pet?v=cat'
curl -i -XPUT 'http://127.0.0.1:8080/pet?v=dog'
curl -i 'http://127.0.0.1:8080/pet'
curl -i 'http://127.0.0.1:8080/pet'
curl -i 'http://127.0.0.1:8080/pet'

Expected:

cat
dog
404 Not Found
Independent queues:
curl -i -XPUT 'http://127.0.0.1:8080/pet?v=cat'
curl -i -XPUT 'http://127.0.0.1:8080/role?v=manager'
curl -i 'http://127.0.0.1:8080/pet'
curl -i 'http://127.0.0.1:8080/role'

Expected:

pet returns cat;
role returns manager.
Missing v:
curl -i -XPUT 'http://127.0.0.1:8080/pet'

Expected:

400 Bad Request.
Timeout with no message:
curl -i 'http://127.0.0.1:8080/pet?timeout=1'

Expected:

waits about 1 second;
returns 404 Not Found.
Timeout with delayed message:

Terminal 1:

curl -i 'http://127.0.0.1:8080/pet?timeout=10'

Terminal 2:

curl -i -XPUT 'http://127.0.0.1:8080/pet?v=cat'

Expected:

terminal 1 receives cat.
FIFO waiters:

Terminal 1:

curl -i 'http://127.0.0.1:8080/pet?timeout=10'

Terminal 2:

curl -i 'http://127.0.0.1:8080/pet?timeout=10'

Terminal 3:

curl -i -XPUT 'http://127.0.0.1:8080/pet?v=cat'
curl -i -XPUT 'http://127.0.0.1:8080/pet?v=dog'

Expected:

terminal 1 receives cat;
terminal 2 receives dog.


If asked to summarize the work, provide:

how to run;
a few curl examples;
approximate clean implementation time.
