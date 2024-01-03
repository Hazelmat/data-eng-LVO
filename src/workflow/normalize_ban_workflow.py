from flytekit import task, workflow

from src.pipeline.ban_normalization_job import run_pipeline


@task
def normalize_ban_task(input_file: str, prefix: str, batch_size: int) -> str:
    output_uri = f"{prefix}/output"
    run_pipeline(input_file, output_uri, batch_size)
    return output_uri


@workflow
def normalize_ban_workflow(input_file: str, prefix: str, batch_size: int) -> str:
    return normalize_ban_task(input_file=input_file, prefix=prefix, batch_size=batch_size)  # type: ignore
