package Homework3;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	double dampingFactor = 0.15;
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String outputadjListOfNode = "";
		Configuration conf = context.getConfiguration();
		double NumberOfNodes = 	Double.parseDouble(conf.get("TOTAL_NUMBER_OF_NODES"));
		double deltaContribution = Double.parseDouble(conf.get("DANGLING_NODES_PAGERANK"));

		boolean isExistingWikiPage = false;
		boolean isDanglingNode = false;
		double pageRankSoFar = 0.0;

		System.out.println("Key :---------------------------"+key.toString());
		for(Text t:values){
			String temp = t.toString();
			//System.out.print(temp);
			if(temp.equals("!")){
				isExistingWikiPage = true;
				continue;
			} 
			if(temp.charAt(0) == '['){
				if (temp == "[]"){
					isDanglingNode = true;
				}
				outputadjListOfNode = temp;
				continue;
			}
			pageRankSoFar += Double.parseDouble(temp);
		}

		if (!isExistingWikiPage){
			return;
		}
		// Update the page rank
		System.out.println("Contribution " + pageRankSoFar);
		double outputPageRank = (dampingFactor / NumberOfNodes) + (1.0 - dampingFactor) * pageRankSoFar;
		context.write(key, new Text(":"+ outputPageRank + ":" + outputadjListOfNode));
		System.out.println("New Page Rank : "+outputPageRank);
		if (isDanglingNode){
		// Add to the dangling contribution for next iteration.
			context.getCounter(nodecounter.DANGLING_NODES_PAGERANK).increment((long) (outputPageRank * Math.pow(10,12)));
		}
	}
}
