package mlops

import org.apache.spark.ml.PipelineModel

object ModelRegistry {

  def register(model: PipelineModel, name: String): Unit = {
    model.write.overwrite().save(s"/models/$name")
  }

  def load(name: String): PipelineModel = {
    PipelineModel.load(s"/models/$name")
  }

}
