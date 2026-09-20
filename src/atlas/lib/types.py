from dataclasses import dataclass
from enum import Enum


class RequestType(Enum):
    CLIENT_SUBMIT_TASK = 0

    SCHEDULER_ACK_TASK = 1
    SCHEDULER_ACK_WORKER_LOGIN = 2

    WORKER_LOGIN = 3
    WORKER_BUSY = 4
    WORKER_TASK_FINISHED = 5
    WORKER_HEARTBEAT = 6


# Client headers
@dataclass
class ClientSubmitTask:
    type = RequestType.CLIENT_SUBMIT_TASK
    task_id: str


type ClientHeader = ClientSubmitTask


# Worker headers
@dataclass
class WorkerLogin:
    type = RequestType.WORKER_LOGIN


@dataclass
class WorkerHeartbeat:
    type = RequestType.WORKER_HEARTBEAT


@dataclass
class WorkerBusy:
    type = RequestType.WORKER_BUSY
    task_id: str


@dataclass
class WorkerFinishedTask:
    type = RequestType.WORKER_TASK_FINISHED
    task_id: str


type WorkerHeader = WorkerLogin | WorkerHeartbeat | WorkerBusy | WorkerFinishedTask


# Scheduler headers
@dataclass
class SchedulerAckWorkerLogin:
    type = RequestType.SCHEDULER_ACK_WORKER_LOGIN


@dataclass
class SchedulerAckTask:
    type = RequestType.SCHEDULER_ACK_TASK


type SchedulerHeader = SchedulerAckWorkerLogin

type RequestHeader = ClientHeader | WorkerHeader | SchedulerHeader


HEADER_REGISTRY: dict[RequestType, type] = {
    RequestType.CLIENT_SUBMIT_TASK: ClientSubmitTask,
    RequestType.SCHEDULER_ACK_TASK: SchedulerAckTask,
    RequestType.SCHEDULER_ACK_WORKER_LOGIN: SchedulerAckWorkerLogin,
    RequestType.WORKER_LOGIN: WorkerLogin,
    RequestType.WORKER_BUSY: WorkerBusy,
    RequestType.WORKER_TASK_FINISHED: WorkerFinishedTask,
    RequestType.WORKER_HEARTBEAT: WorkerHeartbeat,
}


class WorkerStatus(Enum):
    AVAILABLE = 0
    BUSY = 1
