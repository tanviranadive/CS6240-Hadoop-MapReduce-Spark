package Graph.PageRank;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Bz2WikiParserMapper extends Mapper<Object, Text, LongWritable, MatrixCell> {

	private static Pattern namePattern;
	private static Pattern linkPattern;
	private List<String> linkPageNames = new LinkedList<String>();
	private static long numPages;
	private XMLReader xmlReader;
	private Map<String, Long> pageMapper;
	private long totalPages;
	private double initialPageRank;
	private double sinkSum;
	private MultipleOutputs<LongWritable, MatrixCell> mos;
	
	static {
		namePattern = Pattern.compile("^([^~]+)$");
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	public void setup(Context context) {
		try {
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			linkPageNames = new LinkedList<String>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
			
			pageMapper = new HashMap<String, Long>();
			loadPageMapping(context);
			
			totalPages = Long.parseLong(context.getConfiguration().get("totalPages"));
			initialPageRank = 1.0 / totalPages;
			
			mos = new MultipleOutputs<LongWritable, MatrixCell>(context);
			
//			printMap();
			
			sinkSum = 0.0d;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void loadPageMapping(Context context) throws IOException {
		URI[] cacheFiles = context.getCacheFiles();
		if(cacheFiles != null && cacheFiles.length > 0) {
			try {					
				FileSystem fs = FileSystem.get(context.getConfiguration());
				for(URI cacheFile: cacheFiles) {
					readFile(cacheFile, fs);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public void readFile(URI cacheFile, FileSystem fs) {
		try {
			Path path = new Path(cacheFile.toString());
	        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
	        
	        String line;
			
	        while((line = reader.readLine()) != null) {
				String[] parts = line.split("\\s+");
				String pageName = parts[0].trim();
				long id = Long.parseLong(parts[1]);
				pageMapper.put(pageName, id);
			}
			reader.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void printMap() {
		for(String page: pageMapper.keySet()) {
			System.out.println(page + "--->" + pageMapper.get(page));
		}
	}

	public void map(Object o, Text line, Context context) throws IOException, InterruptedException {

		String nextline = line.toString();

		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = nextline.indexOf(':');
		String pageName = nextline.substring(0, delimLoc);
		String html = nextline.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		
		if (matcher.find()) {
			linkPageNames.clear();
			
			try {
				xmlReader.parse(new InputSource(new StringReader(html)));
			} catch (Exception e) {}

			Set<String> outlinksSet = new HashSet<String>();

			for (String link : linkPageNames) {
				if (!link.trim().equals(pageName.trim())) 
					outlinksSet.add(link.trim());
			}

			if(!pageMapper.containsKey(pageName)) return;
			
			long mainPageId = pageMapper.get(pageName);
			
			// generate cells of Matrix-M
			for(String outlink: outlinksSet) {
				if(pageMapper.containsKey(outlink)) {
					long outlinkId = pageMapper.get(outlink);
					MatrixCell mCell = new MatrixCell("M", outlinkId, mainPageId, 0);
						mos.write("output", new LongWritable(mCell.row), mCell, "Matrix-M");
				}
			}
			
			long numOutLinks = outlinksSet.size();  
			if(numOutLinks == 0) { sinkSum += initialPageRank; }
			
			// generate cells of Matrix-R
			MatrixCell rCell = new MatrixCell("R", mainPageId, numOutLinks, initialPageRank);
			mos.write("output", new LongWritable(rCell.row), rCell, "Matrix-R");
		}
	}


	private static class WikiParser extends DefaultHandler {
		private List<String> linkPageNames;
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
	public void cleanup(Context context) throws IOException, InterruptedException {
		long totalSinkSum = (long) (sinkSum * 1000000000);
		context.getCounter("", "SinkSum").increment(totalSinkSum);
		
//		System.out.println("initial sinksum ->" + totalSinkSum);
		
		try {
			mos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}

}
