import dlt
from typing import Dict, Any, List

class PipelineMetrics:
    """Class for collecting and organizing pipeline metrics from pipeline traces."""

    def __init__(self, name: str):
        self.pipeline_name = name
        self.pipeline = dlt.pipeline(pipeline_name=name)
        self.last_trace = self.pipeline.last_trace
        self.metrics: Dict[str, Any] = {}

    def generate_metrics(self) -> Dict[str, Any]:
        if self.last_trace is not None:
            self._build_pipeline_metrics()
        return self.metrics

    def _build_pipeline_metrics(self) -> None:
        status = "processing" if self.last_trace.finished_at is None else "completed"
        self.metrics = {
            "pipeline_name": self.last_trace.pipeline_name,
            "start_time": self.last_trace.started_at,
            "end_time": self.last_trace.finished_at,
            "status": status,
            "metrics": {}
        }

        for step_trace in self.last_trace.steps:
            step_name = step_trace.step
            if step_name == "extract":
                self.metrics["metrics"].update(self._build_step_metrics(step_trace, "table_metrics"))
            elif step_name == "normalize":
                self.metrics["metrics"].update(self._build_step_metrics(step_trace, "resource_metrics"))

    def _build_step_metrics(self, step_trace: Any, metrics_key: str) -> Dict[str, Any]:
        status = "processing" if step_trace.finished_at is None else "completed"
        
        return {
            step_trace.step: {
                "start_time": step_trace.started_at,
                "end_time": step_trace.finished_at,
                "status": status,
                metrics_key: self._build_resource_metrics(step_trace),
                "errors": step_trace.step_exception
            }
        }

    def _build_resource_metrics(self, step_trace: Any) -> List[Dict[str, Any]]:
        resource_metrics = []
        step_info = step_trace.step_info
        
        if hasattr(step_info, 'metrics'):
            for job_id, job_metrics_list in step_info.metrics.items():
                for job_metrics in job_metrics_list:
                    if 'table_metrics' in job_metrics:
                        for table_name, table_metrics in job_metrics['table_metrics'].items():
                            resource_metrics.append({
                                "name": table_name,
                                "row_count": table_metrics.items_count,
                                "file_size": table_metrics.file_size
                            })
        return resource_metrics