import sqlite3
import csv
import json
from tabulate import tabulate
from decimal import Decimal

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'
def init_db():
    conn=sqlite3.connect('choc.db')
    cur=conn.cursor()
    statement='''
        DROP TABLE IF EXISTS 'Bars' ;
    '''
    cur.execute(statement)
    statement='''
        DROP TABLE IF EXISTS 'Countries' ;
    '''
    cur.execute(statement)
    conn.commit()
    statement='''
        CREATE TABLE 'Bars' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Company' TEXT,
            'SpecificBeanBarName' TEXT,
            'REF' TEXT,
            'ReviewDate' TEXT,
            'CocoaPercent' REAL,
            'CompanyLocation' TEXT,
            'CompanyLocationId' INTEGER,
            'Rating' REAL,
            'BeanType' TEXT,
            'BroadBeanOrigin' TEXT,
            'BroadBeanOriginId' INTEGER
        );
    '''
    cur.execute(statement)
    statement='''
        CREATE TABLE 'Countries' (
            'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT,
            'Alpha3' TEXT,
            'EnglishName' TEXT,
            'Region' TEXT,
            'Subregion' TEXT,
            'Population' INTEGER,
            'Area' REAL
        );
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()

def insert_db():
    conn=sqlite3.connect('choc.db')
    cur=conn.cursor()
    with open('flavors_of_cacao_cleaned.csv','r') as f:
        bar_data=csv.reader(f)
        for i in bar_data:
            if i[0] != 'Company':
                statement='''
                    INSERT INTO 'Bars' (Company,SpecificBeanBarName,REF,
                                ReviewDate,CocoaPercent,CompanyLocation,
                                Rating,BeanType,BroadBeanOrigin)
                    VALUES (?,?,?,?,?,?,?,?,?)
                '''
                insertion=(i[0],i[1],i[2],i[3],float(i[4].replace('%',''))/100.0,
                            i[5],i[6],i[7],i[8])
                cur.execute(statement,insertion)
    with open('countries.json','r') as f:
        contents=f.read()
        country_data=json.loads(contents)
        for i in country_data:
            statement='''
                INSERT INTO 'Countries' (EnglishName,Alpha2,Alpha3,Region,Subregion,
                            Population,Area)
                VALUES (?,?,?,?,?,?,?)
            '''
            insertion=(i['name'],i['alpha2Code'],i['alpha3Code'],i['region'],
                        i['subregion'],i['population'],i['area'])
            cur.execute(statement,insertion)
    conn.commit()
    conn.close()
def update_id():
    conn=sqlite3.connect('choc.db')
    cur=conn.cursor()
    statement='''
        UPDATE 'Bars'
        SET CompanyLocationId = ( SELECT Id
                                FROM Countries
                                WHERE Bars.CompanyLocation=Countries.EnglishName),
            BroadBeanOriginId = ( SELECT Id
            					FROM Countries
            					WHERE Bars.BroadBeanOrigin=Countries.EnglishName)
    '''
    cur.execute(statement)
    conn.commit()
    conn.close()
init_db()
insert_db()
update_id()
# Part 2: Implement logic to process user commands

def process_command(command):
    conn=sqlite3.connect('choc.db')
    cur=conn.cursor()

    command=command.split(' ')
    condition_list=[]
# bars
# Description: Lists chocolate bars, according the specified parameters.
# Parameters:
# sellcountry=<alpha2> | sourcecountry=<alpha2> | sellregion=<name> | sourceregion=<name> [default: none]
# Description: Specifies a country or region within which to limit the results, and also specifies whether to limit by the seller (or manufacturer) or by the bean origin source.
# ratings | cocoa [default: ratings]
# Description: Specifies whether to sort by rating or cocoa percentage
# top=<limit> | bottom=<limit> [default: top=10]
# Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
    if 'bars' in command[0]:
        selection = '''SELECT B.SpecificBeanBarName,B.Company,B.CompanyLocation,B.Rating,
        B.CocoaPercent,B.BroadBeanOrigin '''
        statement='FROM Bars AS B'
        order=' ORDER BY B.Rating DESC '
        limit=' LIMIT 10 '

        for i in command:
            if 'sellcountry' in i:
                statement+='''  JOIN Countries AS C
                                ON C.Id = B.CompanyLocationId '''
                j=i.split('=')
                condition=' C.Alpha2 = "'+j[1]+'"'
                condition_list.append(condition)
            if 'sourcecountry' in i:
                statement+='''  JOIN Countries AS C
                                ON C.Id = B.BroadBeanOriginId '''
                j=i.split('=')
                condition=' C.Alpha2 = "'+j[1]+'"'
                condition_list.append(condition)
            if 'sellregion' in i:
                statement+='''  JOIN Countries AS C
                                ON C.Id = B.CompanyLocationId '''
                j=i.split('=')
                condition=' C.Region = "'+j[1]+'"'
                condition_list.append(condition)
            if 'sourceregion' in i:
                statement+='''  JOIN Countries AS C
                                ON C.Id = B.BroadBeanOriginId '''
                j=i.split('=')
                condition=' C.Region = "'+j[1]+'"'
                condition_list.append(condition)
            if 'cocoa' in i:
                order=order.replace('B.Rating','B.CocoaPercent')
            if 'top' in i:
                j=i.split('=')
                top_num=j[1]
                limit=' LIMIT ' + str(top_num)
                # top_num=int(re.search(r'\d+', command).group())
            if 'bottom' in i:
                order=order.replace('DESC','')
                j=i.split('=')
                bottom_num=j[1]
                # bottom_num=int(re.search(r'\d+', command).group())
                limit=' LIMIT ' + str(bottom_num)
        statement=selection+statement
        conditions=' AND '.join(condition_list)
        if conditions:
            statement +=' WHERE ' + conditions
# companies
# Description: Lists chocolate bars sellers according to the specified parameters. Only companies that sell more than 4 kinds of bars are listed in results.
# Parameters:
# country=<alpha2> | region=<name> [default: none]
# Description: Specifies a country or region within which to limit the results.
# ratings | cocoa | bars_sold [default: ratings]
# Description: Specifies whether to sort by rating, cocoa percentage, or the number of different types of bars sold
# top=<limit> | bottom=<limit> [default: top=10]
# Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.

    if 'companies' in command[0]:
        selection=' SELECT B.Company, B.CompanyLocation,AVG(B.Rating) '
        statement='''
                    FROM Bars AS B
                    JOIN Countries AS C
                    ON C.Id = B.CompanyLocationId
                    GROUP BY B.Company
                    HAVING COUNT(B.SpecificBeanBarName) > 4
        '''
        order=' ORDER BY AVG(B.Rating) DESC '
        limit=' LIMIT 10 '
        for i in command:
            if 'bars_sold' in i:
                selection=selection.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
                order=order.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
            if 'cocoa' in i:
                selection=selection.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
                order=order.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
            if 'country' in i:
                j=i.split('=')
                condition=' C.Alpha2 ="'+j[1]+'"'
                condition_list.append(condition)
            if 'region' in i:
                j=i.split('=')
                condition=' C.Region ="'+j[1]+'"'
                condition_list.append(condition)
            if 'top' in i:
                j=i.split('=')
                top_num=j[1]
                limit=' LIMIT ' + str(top_num)
            if 'bottom' in i:
                order=order.replace('DESC','')
                j=i.split('=')
                bottom_num=j[1]
                limit=' LIMIT ' + str(bottom_num)
        statement=selection+statement
        conditions=' AND '.join(condition_list)
        if conditions:
            statement += ' AND ' + conditions
# countries
# Description: Lists countries according to specified parameters. Only countries that sell/source more than 4 kinds of bars are listed in results.
# Parameters:
# region=<name> [default: none]
# Description: Specifies a region within which to limit the results.
# sellers | sources [default: sellers]
# Description: Specifies whether to select countries based sellers or bean sources.
# ratings | cocoa | bars_sold [default: ratings]
# Description: Specifies whether to sort by rating, cocoa percentage, or the number of different types of bars sold
# top=<limit> | bottom=<limit> [default: top=10]
# Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.
    if 'countries' in command[0]:
        selection=' SELECT C.EnglishName,C.Region,AVG(B.Rating) '
        statement1='''
                     FROM Bars AS B
                         JOIN Countries AS C
                         ON C.Id = B.CompanyLocationId
                     GROUP BY B.CompanyLocationId
                     HAVING COUNT(B.SpecificBeanBarName)>4
         '''
        order=' ORDER BY AVG(B.Rating) DESC '
        limit=' LIMIT 10 '
        for i in command:
            if 'region' in i:
                j=i.split('=')
                statement1+=' AND C.Region ="'+j[1]+'"'
            if 'sources' in i:
                statement1=statement1.replace('B.CompanyLocationId','B.BroadBeanOriginId')
            if 'bars_sold' in i:
                selection=selection.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
                order=order.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
            if 'cocoa' in i:
                selection=selection.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
                order=order.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
            if 'top' in i:
                j=i.split('=')
                top_num=j[1]
                limit=' LIMIT ' + str(top_num)
            if 'bottom' in i:
                order=order.replace('DESC','')
                j=i.split('=')
                bottom_num=j[1]
                limit=' LIMIT ' + str(bottom_num)
        statement=selection+statement1
    if 'regions' in command[0]:
        order=' ORDER BY AVG(B.Rating) DESC '
        limit=' LIMIT 10 '
        selection='''
            SELECT C.Region,AVG(B.Rating)
        '''
        statement='''
            FROM Bars AS B
            	JOIN Countries AS C
            	ON C.Id = B.CompanyLocationId
            GROUP BY C.Region
            HAVING COUNT(B.SpecificBeanBarName)>4
        '''
        for i in command:
            if 'bars_sold' in i:
                selection=selection.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
                order=order.replace('AVG(B.Rating)','COUNT(B.SpecificBeanBarName)')
            if 'cocoa' in i:
                selection=selection.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
                order=order.replace('AVG(B.Rating)','AVG(B.CocoaPercent)')
            if 'sources' in i:
                statement=statement.replace('B.CompanyLocationId','B.BroadBeanOriginId')
            if 'top' in i:
                j=i.split('=')
                top_num=j[1]
                limit=' LIMIT ' + str(top_num)
            if 'bottom' in i:
                order=order.replace('DESC','')
                j=i.split('=')
                bottom_num=j[1]
                limit=' LIMIT ' + str(bottom_num)
        statement=selection+statement
    statement += order + limit
    results=cur.execute(statement)
    results_list=results.fetchall()
    conn.close()
    return results_list




def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!

# prompting the user for input
# formatting the output “nicely”
# adding basic error handling (i.e., not crashing the program on invalid inputs)

def convert_to_percent(results_list,command):
    new_list=[]
    command_list=command
    command_list=command_list.split(' ')
    if 'bars' in command_list[0]:
        for i in results_list:
            i=list(i)
            i[-2]=i[-2]*100
            i[-2]=int(i[-2])
            i[-2]=Decimal(i[-2])
            i[-2]=str(i[-2])
            i[-2]+="%"
            new_list.append(i)
    elif 'cocoa' in command:
        for i in results_list:
            i=list(i)
            if type(i[-1]) == float and i[-1]<=1:
                i[-1]=i[-1]*100
                i[-1]=Decimal(i[-1])
                i[-1]=int(i[-1])
                i[-1]=str(i[-1])
                i[-1]+="%"
                new_list.append(i)
    else:
        new_list=results_list

    return new_list

def check_validation(params_list,command):
    words_list=command.split()
    words_list_2 = command.split()
    for i in words_list:
        if i in params_list:
            words_list_2.remove(i)
        elif '=' in i:
            i_split=i.split('=')
            for j in i_split:
                if j in params_list:
                    words_list_2.remove(i)
    if len(words_list_2)==0:
        results=process_command(command)
        percent_results=convert_to_percent(results,command)
        return tabulate(percent_results,floatfmt=".1f")
    else:
        return 0

def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')
        if response == 'help':
            print(help_text)
            continue
        valid_bars_params=['bars','ratings','cocoa','top','bottom','sellcountry',
                        'sellregion','sourcecountry','sourceregion']
        valid_companies_params=['companies','ratings','cocoa','top','bottom',
                                'country','region','bars_sold']
        valid_countries_params=['countries','ratings','cocoa','top','bottom',
                                'region','bars_sold','sellers','sources']
        valid_regions_params=['regions','ratings','cocoa','top','bottom',
                                'bars_sold','sellers','sources']
        output=''
        if 'bars' in response:
            output=check_validation(valid_bars_params,response)
        if 'companies' in response:
            output=check_validation(valid_companies_params,response)
        if 'countries' in response:
            output=check_validation(valid_countries_params,response)
        if 'regions' in response:
            output=check_validation(valid_regions_params,response)

        if output :
            print(output)
            continue
        if output == ''  and (not response):
            continue
        else:
            print('Command not recognized:',response)
            continue

    print('Bye')

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
