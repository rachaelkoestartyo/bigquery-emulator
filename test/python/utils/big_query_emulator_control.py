""" Python automation for Docker, which allows us to start / stop the BigQuery Emulator.

 To start the emulator:
 python -m utils.big_query_emulator_control start

 To stop all running emulators:
 python -m utils.big_query_emulator_control prune_all

 To download the latest image:
 python -m utils.big_query_emulator_control pull_image
 """
import argparse
import logging
import os
import time
from typing import Callable

import attr
from docker import DockerClient, errors
from docker.models.containers import Container

BQ_EMULATOR_PROJECT_ID = "bigquery-emulator-test"


def get_pytest_worker_id() -> int:
    """Retrieves the worker number from the appropriate environment variable
    https://github.com/pytest-dev/pytest-xdist#identifying-the-worker-process-during-a-test
    """
    return int(os.environ.get("PYTEST_XDIST_WORKER", "gw0")[2:])


def get_bq_emulator_port() -> int:
    """Returns the port for the BigQuery emulator that the current environment should
    use. If we are running outside the context of pytest, we assume there is only one
    emulator running at the default port. Otherwise, the port number is variable to
    avoid interference between tests running in parallel.
    """
    # This port is arbitrarily chosen
    port = 60050
    if not (pytest_worker_id := get_pytest_worker_id()):
        return port

    return port + pytest_worker_id


def get_bq_emulator_grpc_port() -> int:
    """Returns the port for the BigQuery emulator that the current environment should
    use. If we are running outside the context of pytest, we assume there is only one
    emulator running at the default port. Otherwise, the port number is variable to
    avoid interference between tests running in parallel.
    """
    # This port is arbitrarily chosen
    port = 61050
    if not (pytest_worker_id := get_pytest_worker_id()):
        return port

    return port + pytest_worker_id


EMULATOR_IMAGE_REPOSITORY = os.getenv("BIGQUERY_EMULATOR_REPOSITORY", "ghcr.io/recidiviz/bigquery-emulator")
EMULATOR_VERSION = os.getenv("BIGQUERY_EMULATOR_VERSION", "latest")
EMULATOR_IMAGE = f"{EMULATOR_IMAGE_REPOSITORY}:{EMULATOR_VERSION}"

EMULATOR_ENTRYPOINT = "/bin/bigquery-emulator"


def _exponential_backoff(
    success_condition: Callable[[], bool],
    delay_ms: int,
    exponent: float,
    max_tries: int,
) -> None:
    tries = 0
    while True:
        tries += 1
        if success_condition():
            break

        if tries == max_tries:
            raise TimeoutError

        time.sleep((delay_ms * (exponent**tries)) / 1000)


@attr.s(auto_attribs=True)
class BigQueryEmulatorControl:
    """Tooling for starting the BigQuery emulator via the Docker API"""

    docker_client: DockerClient = attr.ib()
    container: Container | None = attr.ib(default=None)
    name: str = attr.ib(factory=lambda: f"recidiviz-bq-emulator-{os.urandom(15).hex()}")
    port: int = attr.ib(factory=get_bq_emulator_port)
    grpc_port: int = attr.ib(factory=get_bq_emulator_grpc_port)

    def pull_image(self) -> None:
        try:
            self.docker_client.images.get(EMULATOR_IMAGE)
        except errors.APIError:
            self.docker_client.images.pull(EMULATOR_IMAGE_REPOSITORY, EMULATOR_VERSION)

    def start_emulator(self, input_schema_json_path: str | None = None) -> None:
        """Starts the emulator container. Optionally mounts source tables volume"""
        base_command = (
            f"{EMULATOR_ENTRYPOINT} --project={BQ_EMULATOR_PROJECT_ID} "
            f"--log-level=info --database=:memory: --port={self.port} "
            f"--grpc-port={self.grpc_port}"
        )

        volumes: dict[str, dict[str, str]] = {}

        if input_schema_json_path:
            file_name = os.path.basename(input_schema_json_path)
            base_command += f" --data-from-json=/fixtures/{file_name}"
            volumes = {
                os.path.dirname(input_schema_json_path): {
                    "bind": "/fixtures",
                    "mode": "rw",
                }
            }

        self.container = self.docker_client.containers.run(
            EMULATOR_IMAGE,
            command=base_command,
            name=self.name,
            auto_remove=False,
            ports={self.port: self.port, self.grpc_port: self.grpc_port},
            detach=True,
            volumes=volumes,
        )

        start_time = time.perf_counter()

        try:
            # Exponential backoff, starting at 100ms, ending at 2.3s for a total of ~65 seconds
            _exponential_backoff(
                success_condition=lambda: "REST server listening at 0.0.0.0"
                in self.get_logs(),
                delay_ms=100,
                max_tries=44,
                exponent=1.1,
            )
        except TimeoutError as e:
            raise TimeoutError(
                "Expected emulator to start in less than 60 seconds"
            ) from e

        print(f"Emulator started  in {time.perf_counter() - start_time}s")

    def get_logs(self) -> str:
        if self.container is None:
            raise ValueError("Expected a container in order to print logs")

        logs = self.docker_client.containers.get(self.container.id).logs()

        if isinstance(logs, bytes):
            return logs.decode("utf-8")
        if isinstance(logs, str):
            return logs

        raise ValueError("expected str or bytes type from Docker logs")

    def stop_emulator(self) -> None:
        if self.container:
            self.docker_client.containers.get(self.container.id).stop()

    def prune_all(self) -> None:
        for container in self.docker_client.containers.list(
            filters={
                "status": "running",
                "ancestor": EMULATOR_IMAGE,
            }
        ):
            logging.info("Stopping container: %s", container.name)
            container.stop()

    @classmethod
    def build(cls) -> "BigQueryEmulatorControl":
        return BigQueryEmulatorControl(docker_client=DockerClient.from_env())


ACTION_START = "start"
ACTION_PRUNE = "prune_all"
ACTION_PULL_IMAGE = "pull_image"


def _cli_action(action: str) -> None:
    control = BigQueryEmulatorControl.build()
    if action == ACTION_START:
        control.start_emulator()
    elif action == ACTION_PRUNE:
        control.prune_all()
    elif action == ACTION_PULL_IMAGE:
        control.pull_image()
        logging.info("Pulled image: %s", EMULATOR_IMAGE)
    else:
        raise ValueError(f"Unknown action {action}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=[ACTION_START, ACTION_PULL_IMAGE, ACTION_PRUNE],
    )
    known_args, _ = parser.parse_known_args()
    _cli_action(action=known_args.action)
