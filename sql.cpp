#include <iostream>
#include <queue>
#include <map>
#include <utility>
#include <climits>
#include <stack>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <algorithm>
#include <fstream>

#define ll long long int 
#define er(X) cout<<"error"<<X
#define mp(X,Y) make_pair(X,Y)
#define PI 3.14159265

using namespace std;

	vector<string> tables;
	vector<string> columns;
	vector<string> column1;
	vector<int> column1_val;
	vector<string> column2;
	vector<int> column2_val;
	vector<string> conditions;
	vector<string> cond_column;
	vector<int> cond_column_no;
	vector<string> condval_column;
	vector<string> condsign_column;
	std::vector<string> mulcondval;
	std::vector<string> table1;
	std::vector<string> table2;
	string table1_filename;
	string table2_filename;
	string column1_display;
	string column2_display;

	vector<int> set;

	int common_column1;
	int common_column2;
	int join_table_flag;
	int flag_table;
	int flag_column;
	int error_col;
	int punc_con;
	bool flag;


void reinitialise()
{
	tables.clear();
	columns.clear();
	column1.clear();
	column1_val.clear();
	column2.clear();
	column2_val.clear();
	conditions.clear();
	cond_column.clear();
	cond_column_no.clear();
	condval_column.clear();
	condsign_column.clear();
	mulcondval.clear();
	table1.clear();
	table2.clear();
	set.clear();

	table1_filename="";
	table2_filename="";
	column1_display="";
	column2_display="";


	common_column1=-1;
	common_column2=-1;
	join_table_flag=0;
	flag_table=0;
	flag_column=0;
	error_col=0;
	punc_con=0;

	flag=false;
}


vector< string > parser(string s)
{
	int i=0;
	string a;
	vector <string> parse;
	while(s[i]!='\0')
	{
		if(s[i]==' ' || s[i]==',' || s[i]=='('  ||s[i]==';')
		{
			if(s[i]=='(')
			{
				i++;
				if(a!="" && a!=" ")
					parse.push_back(a);
				a="";
				while(s[i]!=')')
				{
					a+=s[i];
					i++;
				}				
				if(a!="" && a!=" ")
					parse.push_back(a);
				a="";
			}
			else if(s[i]==',' || s[i]==' ')
			{
				if(a!="" && a!=" ")
					parse.push_back(a);
				a="";
			}
		}	
		else
		{
			a+=s[i];
		}

		i++;
	}
	if(a!="")
		parse.push_back(a);
	return parse;
}


vector<string> read(char file[])
{
 	vector<string> list;
	char c;
	string s;	
	if (FILE *fp = fopen(file, "r"))
	{
		while ((c= getc(fp))!=EOF)
			{
				if(c=='\n')
				{
					list.push_back(s);
					s="";
				}
				else
					s+=c;
			}
			if(s!="")
				list.push_back(s);
		fclose(fp);
	}
	return list;
}

void printlist(vector <string> list)
{
	for (int i = 0; i<list.size(); i++)
		{
			if(list[i]=="")cout<<"error"<<i<<" "<<endl;
			else
				cout<<list[i]<<endl;
		}
}

void printlistint(vector<int> v)
{
	for (int i = 0; i <v.size(); ++i)
	{
		cout<<"i = " <<i<<" v[i]="<<v[i]<<endl;		
	}
}

char * stoc(string s)
{
	int i=0;
	int size =s.length();
	char * file = new char[size+1];
	for(i=0 ;i<s.size();i++)
	{
		file[i]=s[i];
	}
	file[i]='\0';
	return file;
}

string ctos(char name[])
{
	string a="";
	for(int i=0;name[i]!='\0';i++)
		a+=name[i];
	return a;
}

int ston(string s)
{
	int i=0,flag=1;
	int sum=0;
	if(s[i]=='-')
	{
		flag=-1;
		i++;		
	}
	sum=0;
	while(s[i]!='\0')
	{
		sum*=10;
		sum+=s[i]-48;
		i++;
	}
	return sum*flag;
}


bool def(string a)
{
	int i=0;
	string tab="";
	while(a[i]!='\0')
	{
		if(a[i]=='.')break;
		tab+=a[i];
		i++;
	}
	if(!flag)
		return false;
	if(tab==column2[0])
		return true;
	else if(tab==column1[0])
		return false;
	else 
	{
		int def_f1=0,def_f2=0;
		i=0;
		while(i<column1.size())
		{
			if(column1[i]==a){
				def_f1=1;
				break;
			}
			i++;
		}
		i=0;
		while(i<column2.size())
		{
			if(column2[i]==a){
				def_f2=1;
				break;				
			}
			i++;			
		}
		if(def_f1==def_f2 && def_f1==1)
		{
			error_col=1;
		}
		else if(def_f2==def_f1 && def_f1==0)
		{
			error_col=1;
		}
		else if(def_f1==1)
			return false;
		else if(def_f2==1)
		{
			return true;
		}
	}

}

string rem(string a)
{
	int i=0;
	string tab="";
	while(a[i]!='\0')
	{
		tab+=a[i];
		if(a[i]=='.')tab="";
		i++;
	}
	return tab;
}


int parse(vector <string> list)
{
		int i=0;
		if(list[0]!="Select")
			return 0;
		i=1;
		while(list[i]!="from")
		{
			columns.push_back(list[i]);
			i++;
		}
		i++;
		if(i<list.size())
		{
			while(list[i]!="where")
			{
				if(list[i]=="join")
				{
					join_table_flag=1;
					i++;
				}
				tables.push_back(list[i]);
				i++;
				if(i==list.size())
						break;
			}	
		}
		i++;
		if(i<list.size())
		{
			while(i<list.size())
			{
				if(list[i]=="AND" || list[i]=="and") mulcondval.push_back("AND");
				else if(list[i]=="OR" || list[i]=="or" ) mulcondval.push_back("OR");
				else
				{
					conditions.push_back(list[i]);
				}
				i++;
			}	
		}
		return 1;
}

vector<string> parse_line(string s)
{
	std::vector<string> v;
	string a;
	int i=0;
	while(s[i]!='\0')
	{	
		if(s[i]==',')
		{
			v.push_back(a);
			a="";
		}
		else
		{
			a+=s[i];
		}	
		i++;		
	}
	if(a!="")
		v.push_back(a);	
	return v;
}

int extract (vector <string> list)
{
	int i;
	int j;
	i=0;
	j=0;
	while(i<list.size())
	{
		if(list[i]==tables[0])
		{
			flag_table++;
			while(list[i]!="<end_table>")
				column1.push_back(list[i++]);
			break;
		}	
		i++;
	}
	if(tables.size()>1)
	{
		while(i<list.size())
		{
			if(list[i]==tables[1])
			{
				flag_table++;
				while(list[i]!="<end_table>")
					column2.push_back(list[i++]);
				break;
			}
			i++;	
		}
	}

	if(flag_table!=tables.size())
		return 0;

	flag =tables.size()>1?true:false;
	if(flag)
	{
		common_column2=0;
		common_column1=-1;
		i=1;
		if(join_table_flag==1)
		{
			while(column1.size()>i)
			{
				j=1;
				while(column2.size()>j)
				{
					if(column1[i]==column2[j])
					{
						common_column1=i;
						common_column2=j;
						break;
					}
					j++;
				}
				if(column2.size()>j)break;	
				i++;
			}
			//cout<<"common_column1 "<<common_column1<<" common_column2 "<<common_column2;
		}
	}
	return 1;
}

vector<string> parse_con(string s)
{
	int i=0;
	string a="";
	vector<string > v;
	while(s[i]!='\0')
	{
		if(s[i]=='=' || s[i]=='<' || s[i]=='>' )
		{
				v.push_back(a);
				a="";
				while(s[i]=='=' || s[i]=='<' || s[i]=='>')
				{
					a+=s[i];
					i++;
				}
				v.push_back(a);
				a="";	
		}
		else
		{
			a+=s[i];
			i++;
		}	
	}
	v.push_back(a);
	return v;
}

bool search(std::vector<int> v,int val,int n)
{
	for (int i = 0; i < n; ++i)
	{
		if(v[i]==val)return true;
	}
	return false;
}

bool join_exist(string a)
{
	int i=0;
	while(a[i]!='\0')
	{
		if(a[i]=='.')
			return true ;	
		i++;
	}
	return false;
}

int extract_condition()
{
	int i=0,k=0;
	std::vector<string> v;	
	while(i<conditions.size())
	{
		v=parse_con(conditions[i]);
		//printlist(v);	
		cond_column.push_back(v[0]);
		condsign_column.push_back(v[1]);
		condval_column.push_back(v[2]);

		if(def(v[0]))
		{
				k=1;
				while(k<column2.size())
				{
					if(column2[k]==rem(v[0]))
						cond_column_no.push_back(k);
					k++;		
				}
		}
		else
		{
				k=1;
				while(k<column1.size())
				{
					if(column1[k]==rem(v[0]))
						cond_column_no.push_back(k);
					k++;		
				}
		}
		i++;
	}
	if(cond_column.size()==cond_column_no.size())
		return error_col==1?0:1;
	else
		return 0;
}

std::vector<string> extract_list(string val2)
{
	std::vector<string> v;
	int k=0,itr_t2=0,itr_t1=0,column_number=0;
	if(def(val2))
	{
		column_number=0;
		k=1;
		while(column2.size()>k)
		{
			if(column2[k]==rem(val2))
			{
				column_number=k;
				break;
			}
			k++;
		}
		itr_t2=0;
		while(table2.size()>itr_t2)
		{
			v.push_back(parse_line(table2[itr_t2])[column_number-1]);
			itr_t2++;
		}

	}	
	else
	{
		column_number=0;
		k=1;
		while(column1.size()>k)
		{
			if(column1[k]==rem(val2))
			{
				column_number=k;
				break;
			}
			k++;
		}
		itr_t1=0;
		while(table1.size()>itr_t1)
		{
			v.push_back(parse_line(table1[itr_t1])[column_number-1]);
			itr_t1++;
		}
	}	
	return v;
}


int eval(string s1,string s2,string sign)
{
	int val1,val2;
	int flag=0;
		val1=ston(s1);
		val2=ston(s2);
		if(sign==">")
		{
			if(val1>val2)flag++;			
		}	
		else if(sign=="<")
		{
			if(val1<val2)flag++;			
		}
		else if(sign==">=")
		{
			if(val1>=val2)flag++;			
		}
		else if(sign=="<=")
		{
			if(val1<=val2)flag++;			
		}
		else if(sign=="=")
		{
			if(val1==val2)flag++;			
		}
	return flag;
}


bool execute_condition(vector<string> v1,vector<string> v2)
{
	int i=0,temp_flag,flag_con;
	string val1 , val2, sign ;
	if(def(cond_column[0]))
	{
		val1=v2[cond_column_no[0]-1];
	}
	else 
	{
		val1=v1[cond_column_no[0]-1];
	}
	val2=condval_column[0];
	sign=condsign_column[0];
	if(join_exist(val2))
	{
		int g=0;
		while(column2.size()>g)
		{
			if(column2[g]==rem(val2))
			{
				val2=v2[g-1];
				break;
			}
			g++;
		}

	}
	flag_con=eval(val1,val2,sign);		
	//cout<<"flag="<<flag_con<<" "<<val1<<" "<<sign<<" "<<val2<<endl;
	i=1;
	while(i<cond_column.size())
	{
		if(def(cond_column[i]))
		{
			val1=v2[cond_column_no[i]-1];
		}
		else 
		{
			val1=v1[cond_column_no[i]-1];
		}
		val2=condval_column[i];
		sign=condsign_column[i];

		temp_flag=eval(val1,val2,sign);
		if(mulcondval[i-1]=="AND")
			flag_con=flag_con&temp_flag;
		else if(mulcondval[i-1]=="OR")
			flag_con=flag_con|temp_flag;
		i++;
	//	cout<<"flag="<<flag_con<<" "<<val1<<" "<<sign<<" "<<val2<<endl;
	}
	if(flag_con==1)
		return true;
	else if(flag_con==0)
		return false;
}


int execute_query()
{

	table1_filename=column1[0]+".csv";
	table1=read(stoc(table1_filename));

	if(flag)
	{
		table2_filename=column2[0]+".csv";
		table2=read(stoc(table2_filename));
	}
	int i=0,k,itr;

	column1_display="";
	column2_display="";

	if(columns[0]=="*")
	{
		k=1;
		while(k<column1.size())
		{
			column1_display+=" "+column1[k];
			column1_val.push_back(k);
			k++;
		}
		if(flag)
		{
			k=1;
			while(k<column2.size())
			{
				column2_display+=" "+column2[k];
				column2_val.push_back(k);
				k++;
			}			
		}

		return 1;
	}
	else 
	{	
		i=0;	
		if(columns[0]=="count" || columns[0]=="sum" || columns[0]=="max" || columns[0]=="min" || columns[0]=="avg" || columns[0]=="distinct")
			i++;
		while(i<columns.size())
		{
			if(def(columns[i]))
			{
				k=1;

				while(k<column2.size())
				{
					if(column2[k]==rem(columns[i]) )
					{
						flag_column++;
						column2_display+=" "+column2[k];
						column2_val.push_back(k);
						break;
					}
					k++;
				}
			}	
			else
			{	k=1;
				while(k<column1.size())
				{
					if(column1[k]==rem(columns[i]))
					{
						flag_column++;
						column1_display+=" "+column1[k];
						column1_val.push_back(k);
						break;
					}
					k++;
				}
			}
			i++;
		}	

		if(columns[0]=="count" || columns[0]=="sum" || columns[0]=="max" || columns[0]=="min" || columns[0]=="avg" || columns[0]=="distinct")
			flag_column++;	
		if(columns.size()==flag_column)
			return error_col==1?0:1;
		else 
			return 0;
	}

}

vector<string> search_row(std::vector<string> v1)
{
	std::vector<string> v2;
	string search=v1[common_column1-1];
	int itr_t2=0;
	while(table2.size()>itr_t2)
	{
		v2=parse_line(table2[itr_t2]);
		if(search==v2[common_column2-1])
			return v2;
		
		v2.clear();
		itr_t2++;
	}
	return v2;
}


void sum_query(string type)
{
	std::vector<string> v1;
	std::vector<string> v2;
	int itr_t1=0,itr_t2=0,i=0;
	int sum=0;
	bool display=true;
	while(itr_t1<table1.size())
	{
		display=true;
		v1=parse_line(table1[itr_t1]);
		if(flag)
			v2=search_row(v1);
		if(conditions.size()>0)
			display=execute_condition(v1,v2);

		if(display==true)
		{
			for (i = 0; i < column1_val.size(); ++i)
				sum+=ston(v1[column1_val[i]-1]);
		}

		itr_t1++;
		itr_t2++;
		v1.clear();
		v2.clear();			
	}
	if(display==true)
	{
		if(type=="sum")
			cout<<"SUM("<<column1_display<<")"<<endl<<sum<<endl;
		else if(type=="avg")
		{
			double d=sum*1.000000;
			cout<<"AVG("<<column1_display<<")"<<endl<<(d/table1.size())<<endl;
		}		
	}
}

string  evaluate_query()
{	
	std::vector<string> v1;
	std::vector<string> v2;
	
	int itr_t1=0;
	int itr_t2=0;
	int i=0;
	string a="";
	int dis_flag;
	bool display;
	int count =0;
	int max=0;
	int min=32000;
	int sum=0;
	int d=0;

	if(columns[0]=="count" || columns[0]=="sum" || columns[0]=="max" || columns[0]=="min" || columns[0]=="avg")
	{

	}	
	else
	{
		cout<<column1_display+column2_display<<endl<<endl;
	}
	while(itr_t1<table1.size())
	{
		display=true;
		v1=parse_line(table1[itr_t1]);

		if(flag)
		{
			if(join_table_flag)
			{
				v2=search_row(v1);
				if(v2.size()>0)
				{
					if(conditions.size()>0)
					{
						display=execute_condition(v1,v2);
					}	
					if(display)
					{
						if(columns[0]=="distinct")
						{
							a="";
							dis_flag=0;
							int val=ston(v1[column1_val[0]-1]);
							if(set.size()>0)
							{
								if(search(set,val,set.size()))
									dis_flag=1;
							}	
							if(dis_flag==0)
							{
								for (i = 0; i < column1_val.size(); ++i)
								
								for (i = 0; i < column2_val.size(); ++i)
										a+=v2[column2_val[i]-1]+" ";

										a+=v1[column1_val[i]-1]+" ";
								set.push_back(ston(v1[column1_val[0]-1]));
								cout<<a<<endl;
							}											
						}
						else if(columns[0]=="count")
						{
							count++;
						}
						else if(columns[0]=="max")
						{
							int val=ston(v1[column1_val[i]-1]);
							if(max<val)max=val;
						}
						else if(columns[0]=="min")
						{
							int val=ston(v1[column1_val[i]-1]);
							if(min>val)min=val;							
						}
						else if(columns[0]=="sum")
						{
							sum+=ston(v1[column1_val[i]-1]);							
						}
						else if(columns[0]=="avg")
						{
							sum+=ston(v1[column1_val[i]-1]);							
						}
						else 
						{
							a="";
							for (i = 0; i < column1_val.size(); ++i)
								a+=" "+v1[column1_val[i]-1];		

							if(flag && v2.size()>0)
								for (i = 0; i < column2_val.size(); ++i)
									a+=" "+v2[column2_val[i]-1];

							if(a!="")
								cout<<a<<endl;				
						}
					}
				}
			}
			else 
			{
				itr_t2=0;
				while(table2.size()>itr_t2)
				{
					v2=parse_line(table2[itr_t2]);
					if(conditions.size()>0)
					{
						display=execute_condition(v1,v2);
					}	
					if(display)
					{
						if(columns[0]=="distinct")
						{
							a="";
							dis_flag=0;
							int val=ston(v1[column1_val[0]-1]);
							if(set.size()>0)
							{
								if(search(set,val,set.size()))
									dis_flag=1;
							}	
							if(dis_flag==0)
							{
								for (i = 0; i < column1_val.size(); ++i)
										a+=v1[column1_val[i]-1]+" ";
								
								for (i = 0; i < column2_val.size(); ++i)
										a+=v2[column2_val[i]-1]+" ";

								set.push_back(ston(v1[column1_val[0]-1]));
								cout<<a<<endl;	
							}						
						}
						else if(columns[0]=="count")
						{
							count++;
						}
						else if(columns[0]=="max")
						{
							int val=ston(v1[column1_val[0]-1]);
							if(max<val)max=val;
						}
						else if(columns[0]=="min")
						{
							int val=ston(v1[column1_val[0]-1]);
							if(min>val)min=val;							
						}
						else if(columns[0]=="sum")
						{
							sum+=ston(v1[column1_val[0]-1]);							
						}
						else if(columns[0]=="avg")
						{
							sum+=ston(v1[column1_val[0]-1]);							
						}
						else 
						{
							a="";
							for (i = 0; i < column1_val.size(); ++i)
								a+=" "+v1[column1_val[i]-1];		

							if(flag && v2.size()>0)
								for (i = 0; i < column2_val.size(); ++i)
									a+=" "+v2[column2_val[i]-1];

							if(a!="")
								cout<<a<<endl;				
						}
					}
					itr_t2++;	
					v2.clear();				
				}
			}
		}
		else 
		{
					v2.clear();
					if(conditions.size()>0)
					{
						display=execute_condition(v1,v2);
					}	
					if(display)
					{
						if(columns[0]=="distinct")
						{
							a="";
							dis_flag=0;
							int val=ston(v1[column1_val[0]-1]);
							if(set.size()>0)
							{
								if(search(set,val,set.size()))
									dis_flag=1;
							}	
							if(dis_flag==0)
							{
								for (i = 0; i < column1_val.size(); ++i)
										a+=v1[column1_val[i]-1]+" ";
								set.push_back(ston(v1[column1_val[0]-1]));
								cout<<a<<endl;
							}											
						}
						else if(columns[0]=="count")
						{
							count++;
						}
						else if(columns[0]=="max")
						{
							int val=ston(v1[column1_val[i]-1]);
							if(max<val)max=val;
						}
						else if(columns[0]=="min")
						{
							int val=ston(v1[column1_val[i]-1]);
							if(min>val)min=val;							
						}
						else if(columns[0]=="sum")
						{
							sum+=ston(v1[column1_val[i]-1]);							
						}
						else if(columns[0]=="avg")
						{
							sum+=ston(v1[column1_val[i]-1]);							
						}
						else 
						{
							a="";
							for (i = 0; i < column1_val.size(); ++i)
								a+=" "+v1[column1_val[i]-1];		
							if(a!="")
								cout<<a<<endl;				
						}
					}
		}
		itr_t1++;
		v1.clear();
		v2.clear();
	}

	if(columns[0]=="count")
	{
		cout<<"COUNT("<<column1_display<<")"<<endl<<count<<endl;
	}
	else if(columns[0]=="max")
	{
		cout<<"MAX("<<column1_display<<")"<<endl<<max<<endl;
	}
	else if(columns[0]=="min")
	{
		cout<<"MIN("<<column1_display<<")"<<endl<<min<<endl;		
	}
	else if(columns[0]=="sum")
	{
		cout<<"SUM("<<column1_display<<")"<<endl<<sum<<endl;		
	}
	else if(columns[0]=="avg")
	{
		double d=sum*1.000000;
		cout<<"AVG("<<column1_display<<")"<<endl<<(d/table1.size())<<endl;
	}

	return a;
}


void printTable(std::vector<string> list)
{
	int i =0;
	while(i<list.size())
	{
		if(list[i]==tables[0])
		{
			flag++;
			while(list[i]!="<end_table>")
				column1.push_back(list[i++]);
			break;
		}	
		i++;
	}
}

int main()
{	
	char c;
	string s;
	int type;	
	string query , filename , query1;
	char str[180];
	vector < string > list ;	
	filename="metadata.txt";

	query1="Select apple.A,B,cherry.C,D  from apple,cherry where cherry.D>1000 and cherry.D<2000";
//	scanf ("%[^\n]%*c", str);

	list=read(stoc(filename));
	int itr=1;
	while(scanf("%[^\n]%*c", str))
	{
		reinitialise();
		cout<<itr<<". "<<endl;
		query=ctos(str);
		parse(parser(query));
		if(extract(list)==1)
		{
			if(extract_condition())
			{
					if(execute_query())
						evaluate_query();			
					else 
						cout<<"column_name : error"<<error_col;
			}
			else
				cout<<"condition_column_name : error"<<error_col;
		}
		else 
		{
			cout<<"Table_name : error";
		}
		reinitialise();
		cout<<endl;
		itr++;
	}
/*
	printlist(column2);
	printlist(table1);
	printlist(table2);
/*	cout<<conditions.size();
	printlist(cond_column);
	printlist(condsign_column);
	printlist(condval_column);
	printlistint(cond_column_no);
	printlist(mulcondval);

//*/
	return 0;
}


