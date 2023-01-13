
# epsy

# HOW TO

To run the `cmp.py` program, first you must install its dependencies. This can be done in a root filesystem with python3.8 installed, or within a virtual environment using virtualenv etc.

`pip3 install -r requirements.txt`

To run the program it requires several parameters to be passed to it. Information about those commands can be found via `python3 ./cmd.py --help`

`python3 ./cmd.py --client_id abcdefg12345 --pool_id us-east-1:6066528c-abcd-1234-5678-56d2d41afdfe --api https://example.appsync-api.us-east-1.amazonaws.com/graphql --bucket_path example_bucket/events.csv`


A prompt for your Cognito username and password will then be output. Please enter those details to execute the program functionality

`Username for Cognito Authentication: test`
`Password for Cognito Authentication`

Statistics will be printed to `stdout` in `JSON` format
Errors will be printed to `stderr`
