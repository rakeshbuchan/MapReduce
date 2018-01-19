package mr.assignment3;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import mr.assignment3.Driver.CountersEnum;

public class PreProcessor {

	private static Pattern namePattern;
	private static Pattern linkPattern;
	private static XMLReader xmlReader;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");

		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// Mapper Class
	public static class PreprocessingJob_Mapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// Parser fills this list with linked page names.
			List<Text> linkPageNames = new LinkedList<Text>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

			String line = value.toString();
			String tilda = "~";

			int delimLoc = line.indexOf(':');
			String pageName = line.substring(0, delimLoc);
			String html = line.substring(delimLoc + 1);
			html = html.replaceAll("&", "&amp;");
			Matcher matcher = namePattern.matcher(pageName);
			if (matcher.find()) {

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// Discard ill-formatted pages.
					e.printStackTrace();
				}

				if (!pageName.contains(tilda)) {
					// emitting page Name and it's outlinks if the name do not contain ~
					context.write(new Text(pageName), new Text(linkPageNames.toString()));
				}

				for (Text linkPageName : linkPageNames) {
					if (!(linkPageName.toString().contains(tilda))) {
						// emitting each of it's outlink pages if it doesn't contain ~
						context.write(new Text(linkPageName), new Text());
					}
				}
			}
		}
	}

	// Combiner Class - helps in reducing multiple emitting of the outlink pages
	// from a single map job - ensures each page is emmitted just once from a single
	// map job
	public static class PreprocessingJob_Combiner extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String result = "";

			for (Text val : values) {
				result = result + val.toString();
			}

			if (result.equals("")) {
				result = "";
			}

			context.write(key, new Text(result));
		}
	}

	// Reducer Class
	public static class PreprocessingJob_Reducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String result = "";

			for (Text val : values) {
				result = result + val.toString();
			}

			if (result.equals("")) {
				result = "[]";
			}
			// counting the number of pages
			Counter counter = (Counter) context.getCounter(CountersEnum.class.getName(),
					CountersEnum.TOTAL_PAGES.toString());
			counter.increment(1);
			// emitting each page with page rank value set to 0.0 at the start
			context.write(key, new Text(result + " #0.0"));
		}
	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<Text> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<Text> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id"))
					&& count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(new Text(matcher.group(1)));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}

}
