from typing import Dict, Any, Sequence
import dlt
from dlt.common.pipeline import LoadInfo, NormalizeInfo, ExtractInfo
from dlt.common.time import ensure_pendulum_datetime
import os
import logging
import gzip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineMetrics:
    def __init__(self, name: str):
        self.pipeline_name = name
        self.pipeline = dlt.pipeline(pipeline_name=name)
        self.last_trace = self.pipeline.last_trace
        self.metrics: Dict[str, Any] = {}

    def generate_metrics(self) -> Dict[str, Any]:
        try:
            self.pipeline.activate()
        except Exception as e:
            logger.error(f"Failed to activate pipeline '{self.pipeline_name}': {e}")
            return self._default_metrics(error=str(e))

        metrics = {
            "pipeline_name": self.pipeline_name,
            "source_type": None,
            "destination_type": self.pipeline.destination.destination_name if self.pipeline.destination else None,
            "start_time": None,
            "end_time": None,
            "status": "unknown",
            "error": None,
            "metrics": {
                "extract": self._get_step_metrics("extract"),
                "normalize": self._get_step_metrics("normalize"),
                "load": self._get_step_metrics("load")
            }
        }

        current_trace = self.pipeline._trace
        if current_trace:
            logger.info(f"Using current trace for pipeline '{self.pipeline_name}'")
            self._update_metrics_from_trace(metrics, current_trace)
        elif self.last_trace:
            logger.info(f"Using last trace for pipeline '{self.pipeline_name}'")
            self._update_metrics_from_trace(metrics, self.last_trace)
        else:
            logger.info(f"Using storage for pipeline '{self.pipeline_name}'")

        return metrics

    def _update_metrics_from_trace(self, metrics: Dict[str, Any], trace: Any) -> None:
        """Update metrics from pipeline trace data"""
        metrics["start_time"] = ensure_pendulum_datetime(trace.started_at)
        metrics["end_time"] = ensure_pendulum_datetime(trace.finished_at) if trace.finished_at else None
        metrics["status"] = "processing" if not trace.finished_at else ("failed" if any(step.step_exception for step in trace.steps) else "completed")

        if metrics["status"] == "failed":
            metrics["error"] = "; ".join(step.step_exception for step in trace.steps if step.step_exception)

        for step in trace.steps:
            step_name = step.step
            if step_name in ["extract", "normalize", "load"]:
                step_metrics = metrics["metrics"][step_name]
                step_metrics["start_time"] = ensure_pendulum_datetime(step.started_at)
                step_metrics["end_time"] = ensure_pendulum_datetime(step.finished_at) if step.finished_at else None
                step_metrics["status"] = "processing" if not step.finished_at else ("failed" if step.step_exception else "completed")
                step_metrics["error"] = step.step_exception
                
                if step.step_info:
                    self._update_step_info_metrics(step_name, step.step_info, step_metrics)


    def _default_metrics(self, error: str = None) -> Dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "source_type": None,
            "destination_type": self.pipeline.destination.destination_name if self.pipeline.destination else None,
            "start_time": None,
            "end_time": None,
            "status": "error" if error else "unknown",
            "error": error,
            "metrics": {
                "extract": self._get_step_metrics("extract"),
                "normalize": self._get_step_metrics("normalize"),
                "load": self._get_step_metrics("load")
            }
        }

    def _get_step_metrics(self, step_name: str) -> Dict[str, Any]:
        return {
            "start_time": None,
            "end_time": None,
            "status": "pending",
            "error": None,
            "resource_metrics": []
        }

    def _update_step_info_metrics(self, step_name: str, step_info: Any, metrics_dict: Dict[str, Any]) -> None:
        if step_name == "extract" and isinstance(step_info, ExtractInfo):
            self._add_extract_metrics(step_info, metrics_dict)
        elif step_name == "normalize" and isinstance(step_info, NormalizeInfo):
            self._add_normalize_metrics(step_info, metrics_dict)
        elif step_name == "load" and isinstance(step_info, LoadInfo):
            self._add_load_metrics(step_info, metrics_dict)

    def _add_extract_metrics(self, step_info: ExtractInfo | Any, metrics_dict: Dict[str, Any]) -> None:
        for load_id, metrics_list in step_info.metrics.items():
            for metrics in metrics_list:
                if "resource_metrics" in metrics:
                    for resource_name, resource_metric in metrics["resource_metrics"].items():
                        logger.info(f"Adding extract metric for '{resource_name}': {resource_metric.items_count} rows, {resource_metric.file_size} bytes")
                        metrics_dict["resource_metrics"].append({
                            "name": resource_name,
                            "row_count": resource_metric.items_count,
                            "file_size": resource_metric.file_size
                        })
                else:
                    logger.warning(f"No resource_metrics found in extract metrics for load_id '{load_id}'")

    def _add_normalize_metrics(self, step_info: NormalizeInfo | Any, metrics_dict: Dict[str, Any]) -> None:
        for load_id, metrics_list in step_info.metrics.items():
            for metrics in metrics_list:
                if "table_metrics" in metrics:
                    for table_name, table_metric in metrics["table_metrics"].items():
                        metrics_dict["resource_metrics"].append({
                            "name": table_name,
                            "row_count": table_metric.items_count,
                            "file_size": table_metric.file_size
                        })

    def _add_load_metrics(self, step_info: LoadInfo | Any, metrics_dict: Dict[str, Any]) -> None:
        for load_id, metrics_list in step_info.metrics.items():
            for metrics in metrics_list:
                if "job_metrics" in metrics:
                    for job_id, job_metric in metrics["job_metrics"].items():
                        metrics_dict["resource_metrics"].append({
                            "name": job_metric.table_name,
                            "start_time": ensure_pendulum_datetime(job_metric.started_at),
                            "end_time": ensure_pendulum_datetime(job_metric.finished_at)
                        })

   