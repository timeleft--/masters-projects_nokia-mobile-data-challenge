package uwaterloo.mdc.etl.weather;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

import uwaterloo.mdc.etl.Config;
import uwaterloo.mdc.etl.Discretize.Sky;
import uwaterloo.mdc.etl.Discretize.Temprature;

public class WeatherUnderGroundDiscretize {
	private WeatherUnderGroundDiscretize() {

	}

	private static Pattern commaSeparator = Pattern.compile("\\,");

	public static class Weather {
		// Some files (e.g: LSGG_1304308800-1304395199.csv) have no data!
		public Sky sky = Sky.Missing;
		public Temprature temprature = Temprature.Missing;
	}

	public static Weather getWeather(long unixTimeGMT, long timeZoneOffset)
			throws IOException {
		Weather result = new Weather();

		long milliTime = unixTimeGMT * 1000;

		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(milliTime);

		Calendar startEndCal = new GregorianCalendar(cal.get(Calendar.YEAR),
				cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH));
		long dayStartSec = startEndCal.getTimeInMillis() / 1000;
		startEndCal.add(Calendar.DAY_OF_MONTH, 1);
		long dayEndSec = (startEndCal.getTimeInMillis() / 1000) - 1;

		String filename = "LSGG_" + dayStartSec + "-" + dayEndSec + ".csv";

		cal.setTimeInMillis(milliTime - timeZoneOffset);

		File dayFile = FileUtils.getFile(Config.PATH_WEATHER, filename);
		BufferedReader dayReader = new BufferedReader(Channels.newReader(
				FileUtils.openInputStream(dayFile).getChannel(), "US-ASCII"));

		String hour = Integer.toString(cal.get(Calendar.HOUR));
		String ampm = (cal.get(Calendar.AM_PM) == Calendar.AM ? "AM" : "PM");

		String line;
		while ((line = dayReader.readLine()) != null) {
			if (line.startsWith(hour)
					&& line.startsWith(ampm, line.indexOf(' ') + 1)) {
				String[] weatherFields = commaSeparator.split(line);

				Double tempC = null;
				if (!weatherFields[1].isEmpty()) {
					tempC = Double.parseDouble(weatherFields[1]);
				}

				// TODO: reduce temprature (percieved) based on wind
				// TODO: Consider gaussian mixture instead of thresholds
				if(tempC == null) {
					continue; // get from next record
				} else if (tempC < -10) {
					result.temprature = Temprature.F;
				} else if (tempC < 5) {
					result.temprature = Temprature.C;
				} else if (tempC < 15) {
					result.temprature = Temprature.M;
				} else if (tempC < 25) {
					result.temprature = Temprature.W;
				} else {
					result.temprature = Temprature.H;
				}

				// TODO?: take visibility into account in Sky
				
				// According to Forecast Description Phrases from
				// http://www.wunderground.com/weather/api/d/documentation.html
				if (weatherFields[11].isEmpty()){
					continue; // get from next record
				} else if (weatherFields[11].contains("Clear")) {
					result.sky = Sky.S;
				} else if (weatherFields[11].contains("Light")) {
					result.sky = Sky.L;
				} else if (weatherFields[11].contains("Heavy")) {
					result.sky = Sky.L;
				} else if (weatherFields[11].contains("Cloud")) {
					result.sky = Sky.C;
				} else if (weatherFields[11].contains("Overcast")) {
					result.sky = Sky.O;
				} else if (weatherFields[11].contains("Fog")
						|| weatherFields[11].contains("Haze")) {
					result.sky = Sky.F;
				} else {
					result.sky = Sky.N;
				}

				break;
			}
		}
		return result;
	}
}
