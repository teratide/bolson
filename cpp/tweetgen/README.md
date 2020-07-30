# An incredibly simple tweet generator.
This generates random JSON formatted tweets as shown [here][1].

[1]:(https://developer.twitter.com/en/docs/labs/sampled-stream/api-reference/get-tweets-stream-sample)

## Install

### Requirements
- CMake 14+
- A C++17 compiler.

### Build
```bash
git clone https://github.com/teratide/flitter.git
cd flitter/cpp/tweetgen
mkdir build && cd build
cmake ..
make
make install
```

### Usage
```
Usage: tweetgen [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  -s,--seed INT               Random generator seed (default: taken from random device).
  -n,--no-tweets UINT         Number of tweets (default = 1).
  -u,--length-mean UINT       Tweet length average.
  -d,--length-deviation UINT  Tweet length deviation.
  -m,--max-referenced-tweets UINT
                              Maximum number of referenced tweets. (default = 2)
  -o,--output TEXT            Output file. If set, JSON file will be written there, and not to stdout.
  -v                          Print the JSON output to stdout, even if -o or --output is used.
```

### Example
```bash
./tweetgen -u 8 -d 0 -s 42
```
Output:
```
[
    {
        "data": {
            "id": "13930160852258120406",
            "created_at": "2014-02-26T03:45:38.000Z",
            "text": "0FMKSXPC",
            "author_id": "6878563960102566144",
            "in_reply_to_user_id": "863363284242328609",
            "referenced_tweets": [
                {
                    "type": "quoted",
                    "id": "2754439531571637074"
                }
            ]
        },
        "format": "compact"
    }
]
```
