import dlt
from ferry.src.destination_factory import DestinationFactory
from ferry.src.source_factory import SourceFactory

class Pipeline:

  def __init__(self, source_uri, destination_uri, source_table, destination_table):
    self.source_uri = source_uri
    self.destination_uri = destination_uri
    self.source_table = source_table
    self.destination_table = destination_table
    self.pipeline = dlt.pipeline(pipeline_name="data_pipeline")


  def build(self):
     destination = DestinationFactory.get(self.destination_uri).dlt_target_system(self.destination_uri)
     source = SourceFactory.get(self.source_uri).dlt_source_system(self.source_uri, self.source_table)
     self.pipeline.extract(source)
     self.pipeline.load(destination)
     return self
  
  def run(self):
    self.pipeline.run()
     

  def __repr__(self):
    return (f"DataPipeline(source_uri={self.source_uri}, "
      f"destination_uri={self.destination_uri}, "
      f"source_table={self.source_table}, "
      f"destination_table={self.destination_table})")
  



  
    

