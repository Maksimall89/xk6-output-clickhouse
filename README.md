# xk6-output-clickhouse
[<img alt="xk6-output-clickhouse" width="400px" src="https://clickhouse.com/images/ch_gh_logo_rounded.png" />](https://clickhouse.com)

[![Build xk6-output-clickhouse](https://github.com/Maksimall89/xk6-output-clickhouse/actions/workflows/go.yml/badge.svg)](https://github.com/Maksimall89/xk6-output-clickhouse/actions/workflows/go.yml)

This is a [k6](https://go.k6.io/k6) extension using the [xk6](https://github.com/grafana/xk6) system.
The extension is `xk6-output-clickhouse` to send k6 metrics and thresholds to [Clickhouse](https://clickhouse.com) in a predefined schema.

| :exclamation: This is a proof of concept, isn't supported by the k6 team, and may break in the future. USE AT YOUR OWN RISK! |
|------|

## Contents
- [Contents](#contents)
- [Build](#build)
    * [Docker build](#docker-build)
- [Usage](#usage)
- [Configure](#configure)
- [Docker Compose](#docker-compose)
    * [Grafana dashboards](#grafana-dashboards)
    * [Clickhouse Config](#clickhouse-config)

## Build

To build a `k6` binary with this extension, first, ensure you have the prerequisites:

- [Go toolchain](https://go101.org/article/go-toolchain.html)
- Git

Then:

1. [Install](https://github.com/grafana/xk6) `xk6`:

  ```shell
  go install go.k6.io/xk6/cmd/xk6@latest
  ```

2. Build the binary:

  ```shell
  CGO_ENABLED=1
  xk6 build --with github.com/Maksimall89/xk6-output-clickhouse@latest
  ```

If you use Windows:

```shell
set CGO_ENABLED=1
xk6 build master --with github.com/Maksimall89/xk6-output-clickhouse@latest
```
### Docker build

This [Dockerfile](./Dockerfile) builds a docker image with the k6 binary.

## Usage

You can run [/examples/script.js](/examples/script.js).

Result output:

```shell
$ ./k6 run -o clickhouse examples/script.js                                       

          /\      |‾‾| /‾‾/   /‾‾/   
     /\  /  \     |  |/  /   /  /    
    /  \/    \    |     (   /   ‾‾\  
   /          \   |  |\  \ |  (‾)  | 
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: examples/script.js
     output: clickhouse

  scenarios: (100.00%) 1 scenario, 15 max VUs, 1m20s max duration (incl. graceful stop):
           * contacts: Up to 15 looping VUs for 1m20s over 4 stages (gracefulRampDown: 0s, gracefulStop: 30s)


     ✗ status code is 500
      ↳  0% — ✓ 0 / ✗ 909

     █ test-group

       ✓ status code is 200

     checks.........................: 49.83% ✓ 903       ✗ 909 
     data_received..................: 4.0 MB 50 kB/s
     data_sent......................: 590 kB 7.4 kB/s
     group_duration.................: avg=374.86ms min=289.48ms med=357.71ms max=892.57ms p(90)=441.02ms p(95)=514.48ms
     http_req_blocked...............: avg=3.75ms   min=0s       med=3µs      max=1.45s    p(90)=6µs      p(95)=8µs     
     http_req_connecting............: avg=1.94ms   min=0s       med=0s       max=347.56ms p(90)=0s       p(95)=0s      
     http_req_duration..............: avg=185.09ms min=142.29ms med=162.8ms  max=981.62ms p(90)=217.35ms p(95)=255.54ms
       { expected_response:true }...: avg=185.09ms min=142.29ms med=162.8ms  max=981.62ms p(90)=217.35ms p(95)=255.54ms
     http_req_failed................: 0.00%  ✓ 0         ✗ 4546
     http_req_receiving.............: avg=157.67µs min=10µs     med=43µs     max=204.43ms p(90)=106µs    p(95)=145µs   
     http_req_sending...............: avg=15.93µs  min=3µs      med=13µs     max=294µs    p(90)=28µs     p(95)=37µs    
     http_req_tls_handshaking.......: avg=1.8ms    min=0s       med=0s       max=1.25s    p(90)=0s       p(95)=0s      
     http_req_waiting...............: avg=184.91ms min=142.2ms  med=162.73ms max=981.58ms p(90)=217.26ms p(95)=255.49ms
     http_reqs......................: 4546   56.826576/s
     iteration_duration.............: avg=944.68ms min=741.92ms med=919.83ms max=2.48s    p(90)=1.13s    p(95)=1.33s   
     iterations.....................: 903    11.287813/s
     vus............................: 1      min=0       max=15
     vus_max........................: 15     min=15      max=15


running (1m20.0s), 00/15 VUs, 903 complete and 15 interrupted iterations
contacts ✓ [======================================] 01/15 VUs  1m20s
```

## Configure

| ENV                         | Default                                       | Description                                                                                                        |
|-----------------------------|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| `K6_CLICKHOUSE_PUSH_INTERVAL` | `15s` (15 second)                               | The flush's frequency of the `k6` metrics. |
| `K6_CLICKHOUSE_DSN`           | `clickhouse://default:pass@localhost:9000/k6DB` | The [Clickhouse Connection string](https://clickhouse.com/docs/en/integrations/sql-clients/cli#connection_string). |

For example:

```shell
k6 run -o clickhouse=clickhouse://default:pass@localhost:9000/k6DB script.js
```

or set an environment variable:

```shell
K6_OUT=clickhouse=clickhouse://default:pass@localhost:9000/k6DB k6 run script.js
```

## Docker Compose

This repo includes a [docker-compose.yml](./docker-compose.yml) file that starts Clickhouse, Grafana, and a custom build of k6 having the `xk6-output-timescaledb` extension.  This is just a quick setup to show the usage for the real use case you might want to deploy outside of docker, use volumes, and probably update versions.

Clone the repo to get started and follow these steps:

1. Put your k6 scripts in the `samples` directory or use the `script.js` example.

3. Start the docker compose environment.

```shell
   docker-compose up -d
```

```shell
[+] Running 4/4
 ⠿ Network xk6-output-clickhouse_clickhouse      Created                                                        0.0s
 ⠿ Container xk6-output-clickhouse-clickhouse-1  Started                                                        0.4s
 ⠿ Container xk6-output-clickhouse-k6-1          Started                                                        0.8s
 ⠿ Container xk6-output-clickhouse-grafana-1     Started                                                        0.8s
```

3. Use the k6 Docker image to run the k6 script and send metrics to the Clickhouse container started in the previous step.

    ```shell
    docker-compose run --rm -T k6 run -<samples/script.js
    ```

   For convenience, the `docker-run.sh` can be used to simply:

    ```shell
    ./docker-run.sh samples/sample.js
    ```

   > Note that the [docker-compose command to run k6 tests](https://k6.io/docs/getting-started/running-k6/) might differ depending your OS.

5. Visit http://localhost:3000/ to view results in Grafana.

### Grafana dashboards

The docker-compose setup comes with one pre-built [dashboard](./grafana/dashboards/Clickhouse%20Load%20Testing%20Results.json) for visualizing the results of a test run.

![Dashboard of test runs](./images/dashboard-1.png)
![Dashboard of test result](./images/dashboard-2.png)

### Clickhouse config

The docker-compose includes config for Clickhouse [user.xml](./clickhouse/users.xml). This config creates `default` user and sets the password - `pass`.