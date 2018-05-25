package Graph.PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Data structure for storing pageRank and oulinks of a page
public class PageData implements WritableComparable<PageData>{
	
	private double pageRank;
	private String outlinks;
	
	public PageData() {
		pageRank=0;
		outlinks="";
	}

	@Override
	public String toString() {
		return (pageRank+" , "+ outlinks);
	}
	
	public void set(double pagerank, String links) {
		this.pageRank = pagerank;
		this.outlinks = links;
	}
	
	public double getPageRank() {
		return this.pageRank;
	}
	
	public void setPageRank(double pr) {
		this.pageRank = pr;
	}
	
	public void setOutlinks(String pages) {
		this.outlinks = pages;
	}
	
	public String getPageList() {
		return this.outlinks;
	}

	public void readFields(DataInput inp) throws IOException {
		// TODO Auto-generated method stub
		this.pageRank = inp.readDouble();
		Text links = new Text();
		links.readFields(inp);
		this.outlinks =  links.toString();
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeDouble(pageRank);
		//System.out.println(this.outlinks);
		Text typ = new Text(this.outlinks);
		typ.write(out);
		
	}

	public int compareTo(PageData pdata) {
		// TODO Auto-generated method stub
		return 0;
	}
}
