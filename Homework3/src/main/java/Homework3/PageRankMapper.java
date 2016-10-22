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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
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

@SuppressWarnings("unused")
public class PageRankMapper extends Mapper<Object, Text, Text, Text>{
	
	double dampingFactor= 0.15;
	
	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// Get values from enum nodecounter
		Configuration conf = context.getConfiguration();
		double NumberOfNodes = 	Double.parseDouble(conf.get("TOTAL_NUMBER_OF_NODES"));
		double PageRankOfDanglingNodes = Double.parseDouble(conf.get("DANGLING_NODES_PAGERANK"));
		String record = value.toString().replaceAll(" ", "");

		String[] tempValues = record.split(":");
		String pageName = tempValues[0].trim();
		double pageRank;

		// PR calculation of present Page
		if(tempValues[1].equals("dummyPageRank")){
			pageRank = 1.0/NumberOfNodes;
		} else {
			pageRank = Double.parseDouble(tempValues[1]);
		}

		// Add delta contribution
		double tempPageRank =  (1- dampingFactor) * ((PageRankOfDanglingNodes/Math.pow(10,12))) / NumberOfNodes;
		pageRank = pageRank + tempPageRank;

		String adjListForOutput = tempValues[2]; // This is to dump the PageName PR:[1,2,3,4,5]

		if (!tempValues[2].equals("[]")){
			// Now Contribute over all adjacency list
			tempValues[2] = tempValues[2].substring(1, tempValues[2].length()-1); // [HARRY,POTTER] -> HARRY,POTTER

			String[] adjacentPages = tempValues[2].split(",");
			double NumberOfOutgoingPages = adjacentPages.length;

			for(String page: adjacentPages){
				Text tempoutKey = new Text(page);
				Text tempOutValue = new Text(Double.toString((pageRank/NumberOfOutgoingPages)));
				context.write(tempoutKey, tempOutValue);
			}
		}

		// Dump the Page Name and its corresponding adjacency list
		String outputString = adjListForOutput;
		Text outputValue = new Text(outputString);
		context.write(new Text(pageName), outputValue);

		// Finally, to check for ghost nodes
		context.write(new Text(pageName), new Text("!"));
	}
}

