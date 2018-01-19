package mr.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*Class which holds the page related details*/

public class PageDetails implements Writable {

	
	private Text pageName;
	private DoubleWritable pageRank;
	private Text outLinks;

	// Constructors
	public PageDetails(Text pageName, DoubleWritable pageRank, Text outLinks) {
		this.pageName = pageName;
		this.pageRank = pageRank;
		this.outLinks = outLinks;
	}

	public PageDetails() {
		this.pageName = new Text("");
		this.pageRank = new DoubleWritable(0.0);
		this.outLinks = new Text("");
	}

	// getters
	public Text getPageName() {
		return pageName;
	}

	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public Text getOutLinks() {
		return outLinks;
	}

	// setters
	public void setPageName(Text pageName) {
		this.pageName = pageName;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}

	public void setOutLinks(Text outLinks) {
		this.outLinks = outLinks;
	}

	// Overridden methods
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.pageName.readFields(in);
		this.pageRank.readFields(in);
		this.outLinks.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		this.pageName.write(out);
		this.pageRank.write(out);
		this.outLinks.write(out);
	}

}
