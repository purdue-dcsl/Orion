

//Copyright (c) 2014 Kanak Mahadik

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE


#include<stdio.h>
#include "/usr/include/stdlib.h"

#include "/usr/include/math.h"
#include <string.h>

//Query file should not have the first line 

int main(int argc, char *argv[])
{

FILE *fquery;
long fdatabase;
const double lambda=1.33;
const double K_ungapped=0.621;
const double H=1.1;
long eff_db,eff_query,act_db,act_query,A;
//Default E value
double E_value=10;
int overlap_length, size_org;
int fragment_length; // can keep as 10k for evaluation purposes
int word_length = 11;
int fragments;

if(argc==7)
{
	if(!strcmp(argv[1],"-query"))
	{
		if(!strcmp(argv[3],"-frag_length"))
		{
			fragment_length=atoi(argv[4]);
			if(!strcmp(argv[5],"-database"))
			{
				
			}
			else 
				printf("\nError. Usage: partition -query query_location -frag_length length -database database_location");
		}
		else
		{
			printf("\nError. Usage: partition -query query_location -frag_length length");
		}
	}
	else
	{
		printf("\nError. Usage: partition -query query_location -frag_length length");
	}
}
else
{
	printf("\nError. Usage: partition -query query_location -frag_length length");
}
fquery=fopen(argv[2],"r");
if (fquery == NULL) 
 { 
 puts ( "Cannot open file" ) ; 
 exit(1) ; 
 }
fseek(fquery,0,SEEK_END);
act_query=ftell(fquery);
fclose(fquery);

printf("\nSize of file at %s is %lu",argv[2],act_query);
fdatabase=atol(argv[6]);
act_db=fdatabase;
printf("\nSize of database at is %lu",act_db);

//Default values of Karlin-Altschul statistcs from : http://drive5.com/usearch/manual/karlin_altschul.html
A = log(act_db*act_query*K_ungapped/H);
eff_db=act_db - A;
eff_query=act_query-A;
overlap_length = 1+(log(K_ungapped*eff_db*eff_query/E_value))/lambda;
if (overlap_length < word_length)
	overlap_length = word_length+1;
printf("\nOverlap length :%d ",overlap_length); 


fragments= 1+ (act_query-fragment_length)/(fragment_length-overlap_length);
//printf("\nFragments number :%d ",fragments); 
int i;
int start,end;
char fname[20]="QueryPart";
char filename[20]="QueryPart";
FILE *fquery_frag;
char ch,buffer[10];
fquery=fopen(argv[2],"r");
if (fquery == NULL) 
 { 
 puts ( "Cannot open file" ) ; 
 exit(1) ; 
 }
 int flag=0;
for(i=0;i<fragments;i++)
{
	flag=1;
	start=(fragment_length-overlap_length)*i+1;
	end  =(fragment_length-overlap_length)*i+fragment_length;
	fseek(fquery,start-1,SEEK_SET);
	snprintf (buffer, sizeof(buffer), "%d",i+1);
	
	strcat(fname,buffer);
	 
	fquery_frag=fopen(fname,"w");
	if (fquery_frag == NULL) 
 { 
 puts ( "Cannot open file" ) ; 
 exit(1) ; 
 }
 ch=fgetc(fquery);
 if(ch!='\n')
 {
 	fseek(fquery,start-1,SEEK_SET);
 }
 while(start<=end)
 {
 	
 	if(flag)
 	{
 		fprintf(fquery_frag,">Query_part_%d\n",i+1);
 		flag=0;
 	}
 	ch=fgetc(fquery);
 	fputc(ch,fquery_frag);	
 	if(ch!='\n')
 		start++;
 }
 fclose(fquery_frag);
 	strcpy(fname,filename);
    printf("\n%d : %d to %d",i+1,(((fragment_length-overlap_length)*i)+1),((fragment_length-overlap_length)*i+fragment_length));
}
flag=1;
start=(fragment_length-overlap_length)*i+1;
end  =act_query;
fseek(fquery,start-1,SEEK_SET);
snprintf (buffer, sizeof(buffer), "%d",i+1);
	
	strcat(fname,buffer);
	 
	fquery_frag=fopen(fname,"w");
	if (fquery_frag == NULL) 
 { 
 puts ( "Cannot open file" ) ; 
 exit(1) ; 
 }
 while(start<=end)
 {
 	if(flag)
 	{
 		fprintf(fquery_frag,">Query_part_%d\n",i+1);
 		flag=0;
 	}
 	
 	ch=fgetc(fquery);
 	fputc(ch,fquery_frag);	
 	start++;
 }
 fclose(fquery_frag);
 
printf("\n%d : %d to %lu",i+1,(((fragment_length-overlap_length)*i)+1),act_query);

printf("\nFragments number :%d ",i+1);
fclose(fquery);
return 0;


}
