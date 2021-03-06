package PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/** Decompresses bz2 file and parses Wikipages on each line. */
public class Bz2WikiParser extends Mapper<Object, Text, Text, PageData> {

	private static Pattern namePattern;
	private static Pattern linkPattern;
	private List<String> linkPageNames = new LinkedList<String>();
	private static long numPages;
	private XMLReader xmlReader;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public void setup(Context context) {
		numPages = 0;
		try {

			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			linkPageNames = new LinkedList<String>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void map(Object o, Text line, Context context) throws IOException, InterruptedException {

		String nextline = line.toString();

		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = nextline.indexOf(':');
		String pageName = nextline.substring(0, delimLoc);
		pageName += " ";
		String html = nextline.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		if (matcher.find()) {

			// Parse page and fill list of linked pages.
			linkPageNames.clear();
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {
				// Discard ill-formatted pages.
			}

			StringBuffer outlinks = new StringBuffer();
			// create a set for outlinks to remove duplicates
			Set<String> outlinksSet = new HashSet<String>();
			outlinks.append("");

			for (String link : linkPageNames) {

				// if a page points to itself, dont include it in the outlinks
				if (!link.trim().equals(pageName.trim())) {
					outlinksSet.add(link.trim());
				}
			}

			// set outlinks from the set
			for (String outlink : outlinksSet) {
				outlinks.append(outlink + " , ");
			}

			String out = outlinks.toString();
			String value;
			if (out.indexOf(",") != -1) {
				value = out.substring(0, out.lastIndexOf(","));
			} else
				value = out;
			numPages++;
			PageData pdata = new PageData();
			pdata.set(0, value);
			context.write(new Text(pageName), pdata);
			//context.write(new Text(pageName), new Text(value));

		}

	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
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
					linkPageNames.add(matcher.group(1));
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

	// cleanup function to set the total number of pages hadoop counter after map
	// tasks have completed
	public void cleanup(Context context) {
		context.getCounter("", "totalPagesCounter").increment(numPages);
	}

}
