package mr.assignment2SS;

import org.apache.hadoop.mapreduce.Partitioner;

//Partitioner Class
//this ensures in routing all the records with same stationID to a same single reducer
public class HashPartitioner extends Partitioner<StationIdYearKey, TemperatureAccumulator> {

	@Override
	public int getPartition(StationIdYearKey stationIdYearKey, TemperatureAccumulator temperatureAccumulator,
			int numPartitions) {
		return (stationIdYearKey.getStationID().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
