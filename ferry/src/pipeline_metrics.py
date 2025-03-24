import dlt
from dlt.common.pipeline import ExtractInfo, NormalizeInfo, LoadInfo

class PipelineMetrics:

  def __init__(self, name: str):
    self.pipeline_name = name
    self.pipeline = dlt.pipeline(pipeline_name=name)
    self.last_trace = self.pipeline.last_trace
    self.metrics = {}


  def generate_metrics(self):
    if self.last_trace is not None:
       self._build_pipeline_metrics()
        
       for step_trace in self.last_trace.steps:  
          step_trace.step_exception
    #     step_name = step_trace.step  # Step name: "extract", "normalize", or "load"          
    #     if(step_name == "extract"):
    #       self.metrics.update(self._build_extract_metrics(step_trace))
    #     elif(step_name == "normalize"):
    #       self.metrics.update(self._build_normalize_metrics(step_trace))
    return self.metrics

  def _build_pipeline_metrics(self):
    status = "processing" if self.last_trace.finished_at == None else "completed"
    self.metrics = {
        "pipeline_name": self.last_trace.pipeline_name,
        "start_time": self.last_trace.started_at,
        "end_time": self.last_trace.finished_at,
        "status": status,
        "metrics": {}
    }
    for step_trace in self.last_trace.steps:
        step_name = step_trace.step
        if(step_name == "extract"):
          self.metrics["metrics"].update(self._build_extract_metrics(step_trace))
        elif(step_name == "normalize"):
          self.metrics["metrics"].update(self._build_normalize_metrics(step_trace))
    
     
       
  def _build_extract_metrics(self, step_trace):
     
     status = "processing" if step_trace.finished_at == None else "completed"
     return {
      step_trace.step: {
        "start_time": step_trace.started_at,
        "end_time": step_trace.finished_at,
        "status": status,
        "table_metrics": self._build_table_metrics(step_trace),
        "errors": step_trace.step_exception}  
      }
    
  
  def _build_normalize_metrics(self,step_trace):
    status = "processing" if step_trace.finished_at == None else "completed"
    return {
      step_trace.step: {
        "start_time": step_trace.started_at,
        "end_time": step_trace.finished_at,
        "status": status,
        "resource_metrics": self._build_table_metrics(step_trace)},
        
    }
  
  def _build_table_metrics(self,step_trace):
    resource_metrics = []
    step_info = step_trace.step_info
    if hasattr(step_info, 'metrics'):
        for job_id, job_metrics_list in step_info.metrics.items():
            for job_metrics in job_metrics_list:
                if 'table_metrics' in job_metrics:
                    print(job_metrics['table_metrics'])
                    for table_name, table_metrics in job_metrics['table_metrics'].items():
                        resource_metrics.append({"name": table_name, "row_count":table_metrics.items_count, "file_size": table_metrics.file_size})
    return resource_metrics
    
                        