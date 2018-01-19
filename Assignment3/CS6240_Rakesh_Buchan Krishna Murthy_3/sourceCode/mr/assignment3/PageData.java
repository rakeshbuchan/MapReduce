package mr.assignment3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

//class which saves page data - page name and the page rank
public class PageData implements Writable, WritableComparable<PageData> {

	private Double pageRank;
	private String pageName;

	// Constructors
	public PageData(Double pageRank, String pageName) {
		this.pageRank = pageRank;
		this.pageName = pageName;
	}

	public PageData() {
		this.pageRank = 0.0;
		this.pageName = "";
	}

	// getters

	public Double getPageRank() {
		return pageRank;
	}

	public String getPageName() {
		return pageName;
	}

	// setters

	public void setPageRank(Double pageRank) {
		this.pageRank = pageRank;
	}

	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

	// overridden methods
	public void write(DataOutput out) throws IOException {
		out.writeDouble(getPageRank());
		out.writeBytes(getPageName());
	}

	public void readFields(DataInput in) throws IOException {
		this.pageRank = in.readDouble();
		this.pageName = in.readLine();
	}

	public int compareTo(PageData pageData) {
		return this.pageRank.compareTo(pageData.getPageRank());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((pageName == null) ? 0 : pageName.hashCode());
		result = prime * result + ((pageRank == null) ? 0 : pageRank.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PageData other = (PageData) obj;
		if (pageName == null) {
			if (other.pageName != null)
				return false;
		} else if (!pageName.equals(other.pageName))
			return false;
		if (pageRank == null) {
			if (other.pageRank != null)
				return false;
		} else if (!pageRank.equals(other.pageRank))
			return false;
		return true;
	}

}
