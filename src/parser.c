
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
//SOFTWARE.



#include<stdio.h>
#include "/usr/include/stdlib.h"
#include<string.h>


//Query file should not have the first line 

int main(int argc, char *argv[])
{

	FILE *fquery,*fqueryout;
	int flag=0;
	char ch;
	if(argc==4)
	{
		if(strcmp(argv[1],"-query"))
			printf("\nError. Usage: parser -query query_location");
	}
	else
	{
		printf("\nError. Usage: parser -query query_location");
	}
	fquery=fopen(argv[2],"r");
	if (fquery == NULL) 
	{ 
		puts ( "Cannot open file" ) ; 
 		exit(1) ; 
	}
	fqueryout=fopen(argv[3],"w");
	if (fqueryout == NULL) 
	{ 
		puts ( "Cannot open file" ) ; 
		exit(1) ; 
	}
	ch=fgetc(fquery);
	while(ch!=EOF)
	{
		if(ch!='\n')
 		{
 			if(flag==1)
	 			fputc(ch,fqueryout);
 	
 		}
		if(ch=='\n')
			flag=1;
 		ch=fgetc(fquery);
	}
 	fclose(fqueryout);
 
	fclose(fquery);
	return 0;
}
