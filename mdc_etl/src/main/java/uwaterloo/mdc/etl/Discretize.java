package uwaterloo.mdc.etl;

public class Discretize {
	private Discretize() {
		// prevents init
	}
	
	public static char duration(long durationInSec){
		long durationInMins = durationInSec / 60;
		char durDiscrete;
		if(durationInMins<10){
			durDiscrete = 't'; //tiny
		} else if(durationInMins<=30){
			durDiscrete='s'; //short
		} else if(durationInMins<=60){
			durDiscrete='m'; //medium
		}else if(durationInMins<=120){
			durDiscrete='l'; //long
		}else if(durationInMins<=240){
			durDiscrete='h'; //half working day
		}else if(durationInMins<=480){
			durDiscrete='w'; //working day
		} else {
			durDiscrete='e'; //epoch
		}
		return durDiscrete;
				
	}
}
