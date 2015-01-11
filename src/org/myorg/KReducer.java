/*
Copyright (c) 2014 Kanak Mahadik 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.*/


package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Reducer.Context;




public class KReducer<K, V>
extends MapReduceBase implements Reducer<K, V, Double, Text> {

	/* Represents the Blast configuration, needs to be modified, according to your configuration:
	private static double K=0.711;
	private static double LAMBDA=1.374;
	private static double H=1.31;
	private static int MATCH_R=1;
	private static int MISMATCH_P=3;
	private static int GAP_INIT_P=5;
	private static int GAP_EXTEND_P=2; */
	
	
	private static double K;
	private static double LAMBDA;
	private static double H;
	private static int MATCH_R;
	private static int MISMATCH_P;
	private static int GAP_INIT_P;
	private static int GAP_EXTEND_P;
	private static float E_THRESHOLD;
	
	private static  int OFFSET;
	private static double QUERY_FRAG_LENGTH;
	private static double QUERY_ACTUAL_LENGTH;
	private static int SCORE_THRESHOLD;
	private static long DB_ACTUAL_LENGTH;
	
	private static double ATSCHUL_CORRECTION;
	private static double DB_CORRECTED_LENGTH;
	private static double QUERY_CORRECTED_LENGTH;
	private static double QUERY_FRAGC_LENGTH;

	
	
	public static class FormattedBlast implements Writable{
		private String db_label;
		private int d_start,d_end;
		private int query_label;
		private int q_start,q_end;
		private int db_length,query_length;
		private int sense;
		private double E_value;
		private int gaps,matches,mismatches;
		private int[] mismatch_map;
		private int[] query_map;
		private int[] db_map;
		private int[] querygap_map;
		private int[] dbgap_map;
		private int gap_init;
		private int gap_extend;


		public FormattedBlast()
		{

		}
		public FormattedBlast(FormattedBlast c)
		{
			d_start=c.d_start;
			d_end=c.d_end;
			query_label=c.query_label;
			q_start=c.q_start;
			q_end=c.q_end;
			db_length=c.db_length;
			query_length=c.query_length;
			sense=c.sense;
			E_value=c.E_value;
			gaps=c.gaps;
			matches=c.matches;
			mismatches=c.mismatches;
			mismatch_map=c.mismatch_map;
			query_map=c.query_map;
			db_map=c.db_map;
			querygap_map=c.querygap_map;
			dbgap_map=c.dbgap_map;
			gap_init=c.gap_init;
			gap_extend=c.gap_extend;
		}
		public void setgapextend(int gap_extend2) {

			gap_extend=gap_extend2;
		}
		public void setquery_label(int l) {

			query_label=l;
		}
		public void setgapinits(int gap_initialize) {
			gap_init=gap_initialize;

		}
		public void setsense(int g) {
			sense=g;

		}
		public int getgapinits() {
			return gap_init;

		}

		public FormattedBlast(Object V)
		{
			int db_ind=0,query_ind=0,mis_ind=0,qgap_ind=0,count=0,dbgap_ind=0;
			int prefix_add;
			{
				String l=new String(V.toString());
				StringTokenizer st=new StringTokenizer(l,",");


				this.db_length=Integer.parseInt(st.nextToken());
				StringTokenizer st1=new StringTokenizer(st.nextToken(),"..");
				d_start=Integer.parseInt(st1.nextToken());
				StringTokenizer st2=new StringTokenizer(st1.nextToken(),"|");
				d_end=Integer.parseInt(st2.nextToken());
				StringTokenizer st4=new StringTokenizer(st2.nextToken(),"_");
				st4.nextToken();
				st4.nextToken();
				query_label=Integer.parseInt(st4.nextToken());
				query_length=Integer.parseInt(st.nextToken());
				st1=new StringTokenizer(st.nextToken(),"..");

				prefix_add=(query_label-1)*OFFSET;

				q_start=prefix_add+Integer.parseInt(st1.nextToken());


				st2=new StringTokenizer(st1.nextToken(),"|");


				q_end=prefix_add+Integer.parseInt(st2.nextToken());

				sense=Integer.parseInt(st2.nextToken());

				String conv=st.nextToken();
				String conv1=new String("1");
				if(conv.startsWith("e"))
					conv1=conv1.concat(conv);
				else
					conv1=conv;
				E_value=Double.parseDouble(conv1);
				StringTokenizer st3=new StringTokenizer(st.nextToken(),"/");
				matches=Integer.parseInt(st3.nextToken());

				st2=new StringTokenizer(st.nextToken(),"\t");
				gaps=Integer.parseInt(st2.nextToken().trim());
				mismatches=Integer.parseInt(st3.nextToken())-matches-gaps;



				querygap_map = new int[gaps];
				dbgap_map = new int[gaps];

				mismatch_map = new int[mismatches];
				query_map = new int[2*(1+gaps)];
				db_map = new int[2*(1+gaps)];


				st2=new StringTokenizer(l,"\t");
				st2.nextToken();
				st1=new StringTokenizer(st2.nextToken(),"|");

				while(st1.hasMoreTokens())
				{
					st2=new StringTokenizer(st1.nextToken(),",");
					st3=new StringTokenizer(st2.nextToken(),":");
					if(st3.countTokens()==1&&gaps!=0)
					{
						gap_init++;
						gap_extend=gap_extend+Integer.parseInt(st3.nextToken())-1;
					}
					else{
						//alignment info


						st4=new StringTokenizer(st3.nextToken(),"..");
						query_map[query_ind++]=prefix_add+Integer.parseInt(st4.nextToken());
						query_map[query_ind++]=prefix_add+Integer.parseInt(st4.nextToken());
						st4=new StringTokenizer(st3.nextToken(),"..");
						db_map[db_ind++]=Integer.parseInt(st4.nextToken());
						db_map[db_ind++]=Integer.parseInt(st4.nextToken());
						while(st2.hasMoreTokens())
						{
							try
							{
								mismatch_map[mis_ind++]=prefix_add+Integer.parseInt(st2.nextToken());
							}
							catch(Exception e)
							{
								this.printElements();
								e.printStackTrace();
							}

						}
					}

				}
				if(gaps>0) //There are gaps
				{
					count=1;
					while((count+2)<db_ind)
					{
						if((db_map[count+1]-db_map[count])==1)//gap in query
						{
							querygap_map[qgap_ind]=query_map[count]+1;
							try{
								while(querygap_map[qgap_ind++]<(query_map[count+1]-1))
								{

									querygap_map[qgap_ind]=querygap_map[qgap_ind-1]+1;
								}
							}

							catch(Exception e)
							{
								this.printElements();
								e.printStackTrace();
							}

						}
						else 
						{
							dbgap_map[dbgap_ind]=prefix_add+db_map[count]+1;
							try
							{

								while(dbgap_map[dbgap_ind++]<(db_map[count+1]-1))
								{
									dbgap_map[dbgap_ind]=prefix_add+dbgap_map[dbgap_ind-1]+1;
								}
							}
							catch(Exception e)
							{
								this.printElements();
								e.printStackTrace();
							}

						}
						count=count+2;

					}
				}

			}
		}
		
		public void setDstart(int a)
		{
			this.d_start=a;
		}
		public void setDend(int a)
		{
			this.d_end=a;
		}
		public void setQstart(int a)
		{
			this.q_start=a;
		}
		public void setQend(int a)
		{
			this.q_end=a;
		}
		public int getDstart()
		{
			return d_start;
		}
		public int getDend()
		{
			return d_end;
		}
		public int getQstart()
		{
			return q_start;
		}
		public int getQend()
		{
			return q_end;
		}
		public int getSense()
		{
			return sense;
		}
		public int getGaps()
		{
			return gaps;
		}
		public int getGapsextend()
		{
			return gap_extend;
		}
		public void setGaps(int g)
		{
			this.gaps=g;
		}
		public int getQLength()
		{
			return query_length;
		}
		public void setQLength(int g)
		{
			this.query_length=g;
		}
		public void setDbLength(int g)
		{
			this.db_length=g;
		}

		public double getEvalue()
		{
			return E_value;
		}

		public int getMatches()
		{
			return matches;
		}
		public void setMatches(int g)
		{
			this.matches=g;
		}

		public int getmisMatches()
		{
			return mismatches;
		}
		public void setmisMatches(int g)
		{
			this.mismatches=g;
		}

		public int[] getmisMatchmap()
		{
			return mismatch_map;
		}
		public void setmisMatchmap(int []box)
		{
			mismatch_map=new int[box.length];
			System.arraycopy(box, 0, mismatch_map, 0,  box.length);
		}
		public void setdbgapmap(int []box)
		{
			dbgap_map=new int[box.length];
			System.arraycopy(box, 0, dbgap_map, 0,  box.length);
		}
		public void setquerygapmap(int []box)
		{
			querygap_map=new int[box.length];
			System.arraycopy(box, 0, querygap_map, 0,  box.length);
		}
		public void setdb_map(int []box)
		{
			db_map=new int[box.length];
			System.arraycopy(box, 0, db_map, 0,  box.length);
		}
		public void setquery_map(int []box)
		{
			query_map=new int[box.length];
			System.arraycopy(box, 0, query_map, 0,  box.length);

		}
		public int[] getquerymap()
		{

			return  query_map;

		}
		public int[] getdbmap()
		{

			return  db_map;

		}

		public int[] getquerygapmap()
		{

			return  querygap_map;

		}
		public int[] getdbgapmap()
		{
			return dbgap_map;
		}

		public FormattedBlast getElement()
		{
			return this;
		}
		public String printElements()
		{
			String part1=new String(this.db_length+","+this.d_start+".."+this.d_end+"|"+"Query_part_1"+","+QUERY_ACTUAL_LENGTH+","+this.q_start+".."+this.q_end+"|"+this.sense+",");


			String putans=new String(" \t|");
			for(int i=0;i<2*(gap_init+1);i=i+2)
			{
				putans=putans.concat(query_map[i]+".."+query_map[i+1]+":"+db_map[i]+".."+db_map[i+1]);
				for(int j=0;j<mismatches;j++)
				{
					if(mismatch_map[j]>=query_map[i]&&mismatch_map[j]<=query_map[i+1])
						putans=putans.concat(","+mismatch_map[j]);
				}
				putans=putans.concat("|");
				if(i+2<query_map.length&&i+2<db_map.length)
				{
					int blanks=0;

					for(int j=0;j<gaps;j++)
					{
						if(j<querygap_map.length)
						{
							if(querygap_map[j]>query_map[i+1]&&querygap_map[j]<query_map[i+2])
							{
								blanks++;
							}
						}
						if(j<dbgap_map.length)
						{	
							if((dbgap_map[j]>=db_map[i+1]&&dbgap_map[j]<=db_map[i+2]))
							{
								blanks++;
							}
						}
						if(blanks>0)
							putans=putans.concat("");
						
					}
				}

			}

			if(this.E_value<0.001)
				part1=part1.concat(String.format("%4.0e", this.E_value));
			else if(this.E_value>1)
				part1=part1.concat(String.format("%4.2f", this.E_value));
			else
				part1=part1.concat(String.format("%5.3f", this.E_value));


			part1=part1.concat(","+this.matches+"/"+(this.mismatches+this.matches+this.gaps)+","+this.gaps+putans);
			return(part1);

		}
		public void setEvalue()
		{

			this.E_value=(QUERY_FRAGC_LENGTH/QUERY_CORRECTED_LENGTH)*E_value;
		}

		public void calculateEvalue()
		{
			double raw_score=matches*MATCH_R-mismatches*MISMATCH_P-GAP_INIT_P*gap_init-GAP_EXTEND_P*gap_extend;
			this.E_value=K*DB_CORRECTED_LENGTH*QUERY_CORRECTED_LENGTH*Math.exp(-LAMBDA*raw_score);
		}
		public int calculateRawscore()
		{
			int raw_score=matches*MATCH_R-mismatches*MISMATCH_P-GAP_INIT_P*gap_init-GAP_EXTEND_P*gap_extend;
			return raw_score;

		}
		public void copyFormattedBlast(FormattedBlast c)
		{
			d_start=c.d_start;
			d_end=c.d_end;
			query_label=c.query_label;
			q_start=c.q_start;
			q_end=c.q_end;
			db_length=c.db_length;
			query_length=c.query_length;
			sense=c.sense;
			E_value=c.E_value;
			gaps=c.gaps;
			matches=c.matches;
			mismatches=c.mismatches;
			mismatch_map=c.mismatch_map;
			query_map=c.query_map;
			db_map=c.db_map;
			querygap_map=c.querygap_map;
			dbgap_map=c.dbgap_map;
			gap_init=c.gap_init;
			gap_extend=c.gap_extend;
		}
	
		@Override
		public void readFields(DataInput arg0) throws IOException {
			d_start=arg0.readInt();

			d_end=arg0.readInt();
			query_label=arg0.readInt();
			q_start=arg0.readInt();
			q_end=arg0.readInt();
			db_length=arg0.readInt();
			query_length=arg0.readInt();
			sense=arg0.readInt();
			E_value=arg0.readDouble();
			gaps=arg0.readInt();
			matches=arg0.readInt();
			mismatches=arg0.readInt();

			int length =arg0.readInt(); 
			mismatch_map=new int[length];
			for(int i=0;i<length;i++)
			{
				mismatch_map[i]=arg0.readInt();
			}

			length =arg0.readInt(); 
			query_map=new int[length];
			for(int i=0;i<length;i++)
			{
				query_map[i]=arg0.readInt();
			}

			length =arg0.readInt(); 
			db_map=new int[length];
			for(int i=0;i<length;i++)
			{
				db_map[i]=arg0.readInt();
			}

			length =arg0.readInt(); 
			querygap_map=new int[length];
			for(int i=0;i<length;i++)
			{
				querygap_map[i]=arg0.readInt();
			}

			length =arg0.readInt(); 
			dbgap_map=new int[length];
			for(int i=0;i<length;i++)
			{
				dbgap_map[i]=arg0.readInt();
			}


			gap_init=arg0.readInt();
			gap_extend=arg0.readInt();
		}
		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(d_start);
			arg0.writeInt(d_end);
			arg0.writeInt(query_label);
			arg0.writeInt(q_start);
			arg0.writeInt(q_end);
			arg0.writeInt(db_length);
			arg0.writeInt(query_length);
			arg0.writeInt(sense);
			arg0.writeDouble(E_value);
			arg0.writeInt(gaps);
			arg0.writeInt(matches);
			arg0.writeInt(mismatches);

			int length = 0;
			if(mismatch_map != null) {
				length = mismatch_map.length;
			}

			arg0.writeInt(length);
			for(int i = 0; i < length; i++) {
				arg0.writeInt(mismatch_map[i]);
			}


			length = 0;
			if(query_map != null) {
				length = query_map.length;
			}

			arg0.writeInt(length);
			for(int i = 0; i < length; i++) {
				arg0.writeInt(query_map[i]);
			}

			length = 0;
			if(db_map != null) {
				length = db_map.length;
			}
			arg0.writeInt(length);
			for(int i = 0; i < length; i++) {
				arg0.writeInt(db_map[i]);
			}

			length = 0;
			if(querygap_map != null) {
				length = querygap_map.length;
			}
			arg0.writeInt(length);
			for(int i = 0; i < length; i++) {
				arg0.writeInt(querygap_map[i]);
			}

			length = 0;
			if(dbgap_map != null) {
				length = dbgap_map.length;
			}
			arg0.writeInt(length);
			for(int i = 0; i < length; i++) {
				arg0.writeInt(dbgap_map[i]);
			}


			arg0.writeInt(gap_init);
			arg0.writeInt(gap_extend);

		}

	}
	public static int[] combine(int [] a,int[] b)
	{


		List<Integer> dintList = new ArrayList<Integer>();
		List<Integer> bintList = new ArrayList<Integer>();
		Set<Integer> set=new HashSet<Integer>();
		for (int index = 0; index < a.length; index++)
		{
			if(a[index]!=0)
				dintList.add(a[index]);
		}
		for (int index = 0; index < b.length; index++)
		{
			if(b[index]!=0)
				bintList.add(b[index]);
		}

		set.addAll(bintList);
		set.addAll(dintList);

		dintList=new ArrayList<Integer>(set);

		int[] ret = new int[dintList.size()];
		for(int index = 0;index < ret.length;index++)
			ret[index] = dintList.get(index);
		Arrays.sort(ret);
		return ret;

	}
	public static int[] merge(int a[],int b[],int interval_start,int interval_end)
	{

		List<Integer> dintList = new ArrayList<Integer>();
		List<Integer> bintList = new ArrayList<Integer>();
		Set<Integer> set=new HashSet<Integer>();
		for (int index = 0; index < a.length; index++)
		{
			if(a[index]!=0||index==0)
				dintList.add(a[index]);

		}
		for (int index = 0; index < b.length; index++)
		{
			if(b[index]!=0||index==0)
				bintList.add(b[index]);
		}

		set.addAll(bintList);
		set.addAll(dintList);

		dintList=new ArrayList<Integer>(set);
		int size;
		size=dintList.size();
		int[] ret = new int[a.length+b.length];
		int[] ret1 = new int[a.length+b.length];

		for(int index = 0;index < dintList.size();index++)
		{
			if (dintList.get(index)!=interval_start&&dintList.get(index)!=interval_end)
				ret[index] = dintList.get(index);
		}

		Arrays.sort(ret);
		System.arraycopy(ret, 0, ret1, 0, size);

		return ret1;
	}
	public static void countgaps(FormattedBlast d)
	{
		int gap_initialize=0;
		int gap_extend=0;
		int prev=0,cur=0;
		if(d.getdbgapmap().length>=1)
			gap_initialize++;

		for(cur=prev+1;cur<d.getdbgapmap().length;cur++)
		{
			if(d.getdbgapmap()[cur]-d.getdbgapmap()[prev]==1)
			{
				gap_extend++;
			}
			else
			{
				gap_initialize++;
			}
			prev++;
		}

		if(d.getquerygapmap().length>=1)
			gap_initialize++;

		prev=0;
		for(cur=prev+1;cur<d.getquerygapmap().length;cur++)
		{
			if(d.getquerygapmap()[cur]-d.getquerygapmap()[prev]==1)
			{
				gap_extend++;
			}
			else
				gap_initialize++;
			prev++;
		}

		d.setgapinits(gap_initialize);
		d.setgapextend(gap_extend);
		d.setGaps(gap_initialize+gap_extend);



	}
	/** Writes all keys and values directly to output. */
	public void reduce(K key, Iterator<V> values,

			OutputCollector<Double, Text> output, Reporter reporter)
		
	throws IOException {
		//System.out.println("OFFSET"+OFFSET+"QUERY_FRAG_LENGTH"+QUERY_FRAG_LENGTH+"QUERY_ACTUAL_LENGTH"+QUERY_ACTUAL_LENGTH+"THRESHOLD"+SCORE_THRESHOLD+"DB_ACTUAL_LENGTH"+DB_ACTUAL_LENGTH+"K"+K+"LAMBDA"+LAMBDA+"H"+H+"MATCH"+MATCH_R+"MISMATCH"+MISMATCH_P+"GAP_INIT"+GAP_INIT_P+"GAP_EXTEND"+GAP_EXTEND_P+"E_THRESHOLD"+E_THRESHOLD);
		
		ArrayList<FormattedBlast> valSetOne = new ArrayList<FormattedBlast>();
		FormattedBlast result;
		FormattedBlast temp;
		long a1,a2,product;
		int flag=0;
		if(OFFSET==0)
		{
			FormattedBlast b=new FormattedBlast(values.next());
			b.calculateEvalue();
		//	if(b.calculateRawscore() >= SCORE_THRESHOLD)
			//if(b.getEvalue()<=E_THRESHOLD)
			{
				output.collect(b.getEvalue(), new Text(key.toString()+","+b.printElements()));
			}
			//else
			{
				//System.out.println("Evlue"+b.getEvalue());
			}

		}
		else
		{
		

		while (values.hasNext()) {
			FormattedBlast b=new FormattedBlast(values.next());
			for(FormattedBlast d : valSetOne)
			{  
				flag=0;
				if(b.getSense()==d.getSense())
				{
					a1=b.getQstart()-d.getQend();
					a2=b.getQend()-d.getQstart();
					product=a1*a2;

					//Same query fragment aligns at different parts in database
					if(b.getQstart()==d.getQstart()&&b.getQend()==d.getQend())
					{
						if(b.getDend()!=d.getDend()&&b.getDstart()!=d.getDstart())
						{
							result=new FormattedBlast(b);
							valSetOne.add(result);
							flag=1;
							break; //Take care of concurrent modification exception
						}
					}
					else if(b.getDstart()==d.getDstart()&&b.getDend()==d.getDend())
					{
						if(b.getQend()!=d.getQend()&&b.getQstart()!=d.getQstart())
						{
							result=new FormattedBlast(b);
							valSetOne.add(result);
							flag=1;
							break; //Take care of concurrent modification exception
						}
					}

					else if(b.getQstart()==d.getQstart()||b.getQend()==d.getQend())

					{
						if(b.getDend()!=d.getDend()&&b.getDstart()!=d.getDstart())
						{
							result=new FormattedBlast(b);
							valSetOne.add(result);
							flag=1;
							break; //Take care of concurrent modification exception
						}
					}
					else if(b.getDend()==d.getDend()||b.getDstart()==d.getDstart())
					{
						if(b.getQend()!=d.getQend()&&b.getQstart()!=d.getQstart())
						{

							result=new FormattedBlast(b);
							valSetOne.add(result);
							flag=1;
							break; //Take care of concurrent modification exception
						}
					}

					else if(product< 0)  // Intersecting or completely contained
					{
						a1=b.getQstart()-d.getQstart();
						a2=b.getQend()-d.getQend();
						product=a1*a2;	
						if(product<0) //completely contained
						{
							if(b.getDend()!=d.getDend()&&b.getDstart()!=d.getDstart())
							{
								result=new FormattedBlast(b);
								valSetOne.add(result);
								flag=1;
								break; //Take care of concurrent modification exception
							}

						}
						else
						{
							//if(b.getDstart()<d.getDend()&&b.getDend()>d.getDend())//Alignments in db overlap
							a1=b.getDstart()-d.getDend();
							a2=b.getDend()-d.getDstart();
							product=a1*a2;
							if(product < 0)
							{
								a1=b.getDstart()-d.getDstart();
								a2=b.getDend()-d.getDend();
								product=a1*a2;	
								if(product<0) //completely contained
								{
									if(b.getQend()!=d.getQend()&&b.getQstart()!=d.getQstart())
									{

										result=new FormattedBlast(b);
										valSetOne.add(result);
										flag=1;
										break; //Take care of concurrent modification exception
									}
								}
								else
								{
									if((b.getQstart()<d.getQstart()&&b.getDstart()<d.getDstart())||(b.getQstart()>d.getQstart()&&b.getDstart()>d.getDstart()))
									{
										int interval_start=0;
										int interval_end=0;

										temp=new FormattedBlast(d);

										d.setmisMatchmap(combine(d.getmisMatchmap(),b.getmisMatchmap()));
										d.setmisMatches(d.getmisMatchmap().length);


										d.setdbgapmap(combine(d.getdbgapmap(),b.getdbgapmap()));
										d.setquerygapmap(combine(d.getquerygapmap(),b.getquerygapmap()));
										countgaps(d);

										interval_start=b.getQstart();
										interval_end=b.getQend();
										if(b.getQstart()-d.getQstart()<0)
										{
											interval_start=d.getQstart();
											d.setQstart(b.getQstart());
										}
										if(b.getQend()-d.getQend()>0)
										{
											interval_end=d.getQend();
											d.setQend(b.getQend());
											//		d.setquery_map(((d.getgapinits()+1)*2)-1,b.getQend());
										}
										d.setquery_map(merge(d.getquerymap(),b.getquerymap(),interval_start,interval_end));


										interval_start=b.getDstart();
										interval_end=b.getDend();

										if(b.getDend()>d.getDend())
										{
											interval_end=d.getDend();
											d.setDend(b.getDend());

										}
										if(b.getDstart()<d.getDstart())
										{
											interval_start=d.getDstart();
											d.setDstart(b.getDstart());

										}

										d.setdb_map(merge(d.getdbmap(),b.getdbmap(),interval_start,interval_end));
										d.setMatches(d.getQend()-d.getQstart()-d.getmisMatches()+1);
										//final alignment is better
										if(d.calculateRawscore()> temp.calculateRawscore()&& b.calculateRawscore()>temp.calculateRawscore()&&d.calculateRawscore()> SCORE_THRESHOLD)
											flag=1;
										else
										{
											flag=0;
											d.copyFormattedBlast(temp);
										}
										break;
									}
								}
							}
						}
					}
				}
			}
			if(flag==0)
			{
				result=new FormattedBlast();
				result.copyFormattedBlast(b);
				valSetOne.add(result);
			}
		}

		for (FormattedBlast it : valSetOne) 
		{
			it.calculateEvalue();
		//	if(it.calculateRawscore() >= SCORE_THRESHOLD)
			if(it.getEvalue()<=E_THRESHOLD)	
				output.collect(it.getEvalue(), new Text(key.toString()+","+it.printElements()));

		}
		}
	}

	public void configure(JobConf job)
	{
		
		
		OFFSET=Integer.parseInt(job.get("OFFSET"));
		QUERY_FRAG_LENGTH=Double.parseDouble(job.get("QUERY_FRAG_LENGTH"));
		QUERY_ACTUAL_LENGTH=Double.parseDouble(job.get("QUERY_ACTUAL_LENGTH"));
		SCORE_THRESHOLD=Integer.parseInt(job.get("THRESHOLD"));
		DB_ACTUAL_LENGTH=Long.parseLong(job.get("DB_ACTUAL_LENGTH"));
		
		K = Double.parseDouble(job.get("K"));
		LAMBDA = Double.parseDouble(job.get("LAMBDA"));
		H = Double.parseDouble(job.get("H"));
		MATCH_R =Integer.parseInt(job.get("MATCH"));
		MISMATCH_P =Integer.parseInt(job.get("MISMATCH"));
		GAP_INIT_P =Integer.parseInt(job.get("GAP_INIT"));
		GAP_EXTEND_P =Integer.parseInt(job.get("GAP_EXTEND"));
		E_THRESHOLD =Float.parseFloat(job.get("E_THRESHOLD"));
		
		ATSCHUL_CORRECTION=Math.log((DB_ACTUAL_LENGTH*QUERY_ACTUAL_LENGTH*K/H));
		DB_CORRECTED_LENGTH=DB_ACTUAL_LENGTH-ATSCHUL_CORRECTION;;
		QUERY_CORRECTED_LENGTH=QUERY_FRAG_LENGTH-ATSCHUL_CORRECTION;;
		QUERY_FRAGC_LENGTH=QUERY_FRAG_LENGTH-ATSCHUL_CORRECTION;;

	}
}


