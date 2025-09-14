package algorithm

import org.scalatest.funsuite.AnyFunSuite

class DBSCAN_MSTest extends AnyFunSuite{
  test("Basic 3D test") {
    DBSCAN_MS.run("data/synth_data_points10000x3D.csv", epsilon = 0.03f, minPts = 3, numberOfPivots = 9, numberOfPartitions = 10, samplingDensity = 0.2f)
  }


}
