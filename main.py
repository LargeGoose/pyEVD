# Python script to take the standard EVD output and create a SQLite DB
# Version 1.0 - August 2nd 2022

# Set the evd_files_location variable to the directory wheer your EVD export files are
evd_files_location = 'C:\\EVD\\'
# Optionally change the list of reports expected. I wouldn't do that, I'd change your report names
list_of_reports = ['FilesList', 'LogList', 'OwnersList', 'RequestsList', 'SafesList', 'GroupsList', 'GroupMembersList',
                   'UsersList', 'LocationsList', 'ConfirmationsList', 'Italogfile', 'EventsList', 'ObjectProperties',
                   'MasterPolicySettings']

import datetime
import os
import csv
import re
from sqlite3 import connect
from sqlite3 import Error
from time import sleep
from tqdm import tqdm

def create_table(list):
    global sql
    if list == "FilesList":
        # Create the Files table
        sql = '''CREATE TABLE Files (
        SafeID INTEGER,
        SafeName TEXT,
        Folder TEXT,
        FileID INTEGER,
        FileName TEXT,
        InternalName TEXT,
        Size INTEGER,
        CreatedBy TEXT,
        CreationDate TEXT,
        LastUsedBy TEXT,
        LastUsedDate TEXT,
        ModificationDate TEXT,
        ModifiedBy TEXT,
        DeletedBy TEXT,
        DeletionDate TEXT,
        LockDate TEXT,
        LockBy TEXT,
        LockedByUserID INTEGER,
        Accessed TEXT,
        New TEXT,
        Retrieved TEXT,
        Modified TEXT,
        IsRequestNeeded TEXT,
        ValidationStatus INTEGER,
        Type INTEGER,
        CompressedSize INTEGER,
        LastModifiedDate TEXT,
        LastModifiedBy TEXT,
        LastUsedByHuman TEXT,
        LastUsedHumanDate TEXT,
        LastUsedByComponent TEXT,
        LastUsedComponentDate TEXT
        );
        CREATE INDEX FilesIND ON Files(SafeID, FileID);
        '''
    elif list == "LogList":
        # Create the Log table
        sql = '''CREATE TABLE Log (
        MasterID INTEGER,
        LogID INTEGER,
        Type INTEGER,
        Code INTEGER,
        Time STRING,
        Action STRING,
        SafeID INTEGER,
        SafeName TEXT,
        UserID INTEGER,
        UserName TEXT,
        Info1ID INTEGER,
        Info1Type INTEGER,
        Info1 TEXT,
        Info2ID INTEGER,
        Info2Type INTEGER,
        Info2 TEXT,
        RequestID INTEGER,
        RequestReason TEXT,
        Alert TEXT,
        InterfaceId TEXT,
        AppAuditDate TEXT,
        ExternalAudit TEXT,
        UserTypeID INTEGER
        );
        CREATE INDEX LogsIND ON Log(MasterID);
        '''
    elif list == "OwnersList":
        # Create the Owners table
        sql = '''CREATE TABLE Owners (
        SafeID INTEGER,
        SafeName TEXT,
        OwnerID INTEGER,
        OwnerName TEXT,
        OwnerType INTEGER,
        ExpirationDate TEXT,
        List TEXT,
        Retrieve TEXT,
	    CreateObject TEXT,
	    UpdateObject TEXT,
	    UpdateObjectProperties TEXT,
	    RenameObject TEXT,
	    [Delete] TEXT,
	    ViewAudit TEXT,
	    ViewOwners TEXT,
	    UsePassword TEXT,
	    InitiateCPMChange TEXT,
	    InitiateCPMChangeWithManualPassword TEXT,
	    CreateFolder TEXT,
	    DeleteFolder TEXT,
	    UnlockObject TEXT,
	    MoveFrom TEXT,
	    MoveInto TEXT,
	    ManageSafe TEXT,
	    ManageSafeOwners TEXT,
	    ValidateSafeContent TEXT,
	    Backup TEXT,
	    NoConfirmRequired TEXT,
	    Confirm TEXT,
	    EventsList TEXT,
	    EventsAdd TEXT
	    );
	    CREATE INDEX OwnersIND ON Owners(SafeID, OwnerID);
	    '''
    elif list == "RequestsList":
        # Create the Requests table
        sql = '''CREATE TABLE Requests (
        RequestID INTEGER,
        UserID INTEGER,
        UserName TEXT,
        Type INTEGER,
        SafeID INTEGER,
        SafeName TEXT,
        FolderName TEXT,
        FileID INTEGER,
        FileName TEXT,
        Reason TEXT,
        AccessType INTEGER,
        ConfirmationsCount INTEGER,
        ConfirmationsLeft INTEGER,
        RejectionsCount INTEGER,
        InvalidReason INTEGER,
        CreationDate TEXT
        ExpirationDate TEXT,
        PeriodFrom TEXT,
        PeriodTo TEXT,
        LastUsedDate TEXT,
        Status INTEGER
        );
        CREATE INDEX RequestsIND ON Requests(RequestID, SafeID);
        '''
    elif list == "SafesList":
        # Create the Safes table
        sql = '''CREATE TABLE Safes (
        SafeID INETEGER,
        SafeName TEXT,
        LocationID INTEGER,
        LocationName TEXT,
        Size INTEGER,
        MaxSize INTEGER,
        [%UsedSize] INTEGER,
        LastUsed TEXT,
        VirusFree TEXT,
        TextOnly TEXT,
        AccessLocation INTEGER,
        SecurityLevel INTEGER,
        Delay INTEGER,
        FromHour INTEGER,
        ToHour INTEGER,
        DailyVersions INTEGER,
        MonthlyVersions INTEGER,
        YearlyVersions INTEGER,
        LogRetentionPeriod INTEGER,
        ObjectsRetentionPeriod INTEGER,
        RequestRetentionPeriod INTEGER,
        ShareOptions INTEGER,
        ConfirmersCount INTEGER,
        ConfirmType INTEGER,
        DefaultAccessMarks INTEGER,
        DefaultFileCompression TEXT,
        DefaultReadOnly TEXT,
        QuotaOwner TEXT,
        UseFileCategories TEXT,
        RequireReasonToRetrieve TEXT,
        EnforceExlusivePasswords TEXT,
        RequireContentValidation TEXT,
        CreationDate TEXT,
        CreatedBy TEXT,
        NumberOfPasswordVersions INTEGER
        );
        CREATE INDEX SafesIND ON Safes(SafeID);
        '''
    elif list == "GroupsList":
        # Create the Groups table
        sql = '''CREATE TABLE Groups (
        GroupID INTEGER,
        GroupName TEXT,
        LocationID INTEGER,
        LocationName TEXT,
        Description TEXT,
        ExternalGroupName TEXT,
        InternalExternal INTEGER,
        LDAPFullDN TEXT,
        LDAPDirectory TEXT,
        MapName TEXT,
        MapID INTEGER
        );
        CREATE INDEX GroupsIND ON Groups(GroupID);
        '''
    elif list == "GroupMembersList":
        # Create the GroupMembers table
        sql = '''CREATE TABLE GroupMembers (
        GroupID INTEGER,
        UserID INTEGER,
        MemberIsGroup TEXT
        );
        CREATE  INDEX GroupMembersIND ON GroupMembers(GroupID, UserID);
        '''
    elif list == "UsersList":
        # Create the Users table
        sql = '''CREATE TABLE Users (
        UserID INTEGER,
        UserName TEXT,
        LocationID INTEGER,
        LocationName TEXT,
        FirstName TEXT,
        LastName TEXT,
        BusinessEmail TEXT,
        Disabled TEXT,
        FromHour INTEGER,
        ToHour INTEGER,
        ExpirationDate TEXT,
        PasswordNeverExpires TEXT,
        LogRetentionPeriod INTEGER,
        AuthenticationMethods INTEGER,
        Authorizations INTEGER,
        GatewayAccountAuthorizations INTEGER,
        DistinguishedName TEXT,
        InternalExternal INTEGER,
        LDAPFullDN TEXT,
        LDAPDirectory TEXT,
        MapName TEXT,
        MapID INTEGER,
        LastLogonDate TEXT,
        PrevLogonDate TEXT,
        UserTypeID INTEGER,
        RestrictedInterfaces TEXT,
        ApplicationMetadata TEXT,
        CreationDate TEXT
        );
        CREATE INDEX UsersIND ON Users(UserID);
        '''
    elif list == "LocationsList":
        # Create the Locations table
        sql = '''CREATE TABLE Locations (
        LocationID INTEGER,
        LocationName TEXT
        );
        CREATE  INDEX LocationsIND ON Locations(LocationID);
        '''
    elif list == "ConfirmationsList":
        # Create the Confirmations table
        sql = '''CREATE TABLE Confirmations (
        RequestID INTEGER,
        SafeID INTEGER,
        SafeName TEXT,
        UserID INTEGER,
        UserName TEXT,
        GroupID INTEGER,
        GroupName TEXT,
        Reason TEXT,
        Action INTEGER,
        ActionDate TEXT
        );
        CREATE  INDEX ConfirmationsIND ON Confirmations(RequestID, SafeID, UserID);
        '''
    elif list == "Italogfile":
        # Create the ITALog table
        sql = '''CREATE TABLE ITALog (
        Time TEXT,
        Code TEXT,
        LogMessage TEXT
        );
        CREATE  INDEX ITALogIND ON ITALog(Time);
        '''
    elif list == "EventsList":
        # Create the Events table
        sql = '''CREATE TABLE Events (
        SafeName TEXT,
        EventID INTEGER,
        SourceID INTEGER,
        EventTypeID INTEGER,
        ClientID TEXT,
        UserName TEXT,
        AgentName TEXT,
        FromIP TEXT,
        Version TEXT,
        CreationDate TEXT,
        ExpirationDate TEXT,
        EventVersion INTEGER,
        Data TEXT
        );
        CREATE INDEX EventsIND ON Events(EventID);
        '''
    elif list == "ObjectProperties":
        # Create the ObjectProperties table
        sql = '''CREATE TABLE ObjectProperties (
        ObjectPropertyId INTEGER,
        ObjectPropertyName TEXT,
        SafeId INTEGER,
        FileId INTEGER,
        ObjectPropertyValue TEXT,
        Options INTEGER
        );
        CREATE INDEX ObjectPropertiesIND ON ObjectProperties(ObjectPropertyId, SafeId, FileId);
        '''
    elif list == "MasterPolicySettings":
        # Create the MasterPolicySettings table
        sql = '''CREATE TABLE MasterPolicySettings (
        PolicyName TEXT,
        MasterPolicySettingName TEXT,
        MasterPolicySettingValue TEXT,
        MasterPolicyAdvanceSettingName TEXT,
        MasterPolicyAdvanceSettingValue TEXT,
        IsException TEXT
        )'''
    elif list == "TextCodes":
        # Create the TextCodes table
        sql = '''CREATE TABLE TextCodes (
        Type INTEGER,
        Code INTEGER,
        Text TEXT
        );
        CREATE INDEX TextCodesIND ON TextCodes(Type, Code);
        INSERT INTO TextCodes VALUES(1,1,'Password');
        INSERT INTO TextCodes VALUES(1,2,'PKI');
        INSERT INTO TextCodes VALUES(1,4,'SECUREID');
        INSERT INTO TextCodes VALUES(1,8,'NTAuth');
        INSERT INTO TextCodes VALUES(1,16,'RADIUS');
        INSERT INTO TextCodes VALUES(2,0,'None');
        INSERT INTO TextCodes VALUES(2,1,'Users Administrators');
        INSERT INTO TextCodes VALUES(2,2,'Safes Administrators');
        INSERT INTO TextCodes VALUES(2,4,'Network Area Administrators');
        INSERT INTO TextCodes VALUES(2,8,'User Templates Administrators');
        INSERT INTO TextCodes VALUES(2,16,'File Categories Administrators');
        INSERT INTO TextCodes VALUES(2,32,'Autdit All');
        INSERT INTO TextCodes VALUES(2,64,'Backup All');
        INSERT INTO TextCodes VALUES(2,128,'Restore All');
        INSERT INTO TextCodes VALUES(3,0,'None');
        INSERT INTO TextCodes VALUES(3,1,'Full');
        INSERT INTO TextCodes VALUES(3,2,'Partial');
        INSERT INTO TextCodes VALUES(3,4,'LogonAs');
        INSERT INTO TextCodes VALUES(4,1,'Internal');
        INSERT INTO TextCodes VALUES(4,2,'External');
        INSERT INTO TextCodes VALUES(5,1,'Internal');
        INSERT INTO TextCodes VALUES(5,2,'External');
        INSERT INTO TextCodes VALUES(5,4,'Public (Internet)');
        INSERT INTO TextCodes VALUES(6,8,'Unsecured');
        INSERT INTO TextCodes VALUES(6,16,'Secure');
        INSERT INTO TextCodes VALUES(6,32,'Highly Secured');
        INSERT INTO TextCodes VALUES(7,0,'None');
        INSERT INTO TextCodes VALUES(7,1,'Require Full Impersonation');
        INSERT INTO TextCodes VALUES(7,2,'Require Partial Impersonation');
        INSERT INTO TextCodes VALUES(7,4,'Require LogonAs Impersonation');
        INSERT INTO TextCodes VALUES(7,8,'Require Authentication And Open');
        INSERT INTO TextCodes VALUES(8,0,'None');
        INSERT INTO TextCodes VALUES(8,1,'Open Safe');
        INSERT INTO TextCodes VALUES(8,2,'Get File');
        INSERT INTO TextCodes VALUES(8,3,'Open And Get');
        INSERT INTO TextCodes VALUES(9,0,'None');
        INSERT INTO TextCodes VALUES(9,1,'Accessed');
        INSERT INTO TextCodes VALUES(9,2,'New');
        INSERT INTO TextCodes VALUES(9,4,'Modified');
        INSERT INTO TextCodes VALUES(9,7,'All');
        INSERT INTO TextCodes VALUES(10,0,'User');
        INSERT INTO TextCodes VALUES(10,1,'Group');
        INSERT INTO TextCodes VALUES(10,2,'Gateway account');
        INSERT INTO TextCodes VALUES(11,1,'Pending');
        INSERT INTO TextCodes VALUES(11,2,'Valid');
        INSERT INTO TextCodes VALUES(11,4,'Invalid');
        INSERT INTO TextCodes VALUES(12,1,'File');
        INSERT INTO TextCodes VALUES(12,2,'Password');
        INSERT INTO TextCodes VALUES(13,2,'User log record');
        INSERT INTO TextCodes VALUES(13,3,'Safe log record');
        INSERT INTO TextCodes VALUES(14,0,'None');
        INSERT INTO TextCodes VALUES(14,1,'User');
        INSERT INTO TextCodes VALUES(14,2,'Location');
        INSERT INTO TextCodes VALUES(14,3,'File/Password');
        INSERT INTO TextCodes VALUES(14,4,'Network area');
        INSERT INTO TextCodes VALUES(14,5,'Category');
        INSERT INTO TextCodes VALUES(15,1,'Open Safe');
        INSERT INTO TextCodes VALUES(15,2,'Get File');
        INSERT INTO TextCodes VALUES(15,4,'Get Password');
        INSERT INTO TextCodes VALUES(16,0,'One time access');
        INSERT INTO TextCodes VALUES(16,1,'Multiple access');
        INSERT INTO TextCodes VALUES(17,0,'None');
        INSERT INTO TextCodes VALUES(17,1,'Expired');
        INSERT INTO TextCodes VALUES(17,2,'Already Used');
        INSERT INTO TextCodes VALUES(17,4,'Damaged - Missing supervisor');
        INSERT INTO TextCodes VALUES(17,8,'Damaged - Confirmation settings changes');
        INSERT INTO TextCodes VALUES(17,16,'Damaged – Object deleted');
        INSERT INTO TextCodes VALUES(17,32,'Damaged – Incompatible version');
        INSERT INTO TextCodes VALUES(17,64,'ToDate passed');
        INSERT INTO TextCodes VALUES(18,1,'Waiting');
        INSERT INTO TextCodes VALUES(18,2,'Confirmed');
        INSERT INTO TextCodes VALUES(19,0,'None');
        INSERT INTO TextCodes VALUES(19,1,'Reject');
        INSERT INTO TextCodes VALUES(19,2,'Confirm')
        '''
    cursor.executescript(sql)
    evd_mem_db.commit()
    sql = None

start = datetime.datetime.now()
files_count = len([name for name in os.scandir(evd_files_location) if os.path.isfile(name)])
file_count = 0
file_names = []
print(f'Scanning the files in {evd_files_location}')
for entry in os.scandir(evd_files_location):
    if entry.is_file():
        file_count += 1
        # Test to see if the file is expected from the name
        for report in list_of_reports:
            if report in entry.name:
                # print(f'Found report name match for "{report}"')
                list_of_reports.remove(report)
                file_names.append(entry.name)
print(f'{file_count} files in directory {evd_files_location} processed')
if len(list_of_reports) == 0:
    print("All the EVD Reports required were found")
else:
    print(f'These report(s) will not be processed {list_of_reports} as they were not found')
evd_mem_db = None
try:
    evd_mem_db = connect(':memory:')
    cursor = evd_mem_db.cursor()
except Error as e:
    print(e)
create_table("TextCodes")
for evd_file in file_names:
    if "FilesList" in evd_file:
        create_table("FilesList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Files ({row}) VALUES ("
                else:
                    # Join the elements of the row with ," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # The CompressedSize column seems to be always badly formatted with an extra double quote
                    # RegEx search for one more more digits followed by two double quotes and remove the second quote
                    row = re.sub('"-?(\d+)""', '"\\1"', row)
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "LogList" in evd_file:
        create_table("LogList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Log ({row}) VALUES ("
                else:
                    # If the "RequestReason" field has a complex value when adding a File Category the content contains double quotes and needs wrapping in single quotes
                    if "Value=[{" in row[17]:
                        row[17] = f'\'{row[17]}\''
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Remove the double quotes around element 18
                        row = re.sub('"(\'Value=\[{.+}\]\')"', '\\1', row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    else:
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "OwnersList" in evd_file:
        create_table("OwnersList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # Wrap the "Delete" header field with [] as it's a reserved word
                    row = re.sub('(Delete),', '[\\1],', row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Owners ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "RequestsList" in evd_file:
        create_table("RequestsList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # Wrap the "Delete" header field with [] as it's a reserved word
                    row = re.sub('(Delete),', '[\\1],', row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Requests ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "SafesList" in evd_file:
        create_table("SafesList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # Wrap the "%UsedSize" header field with [] as % is not allowed
                    row = re.sub('(%UsedSize),', '[\\1],', row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Safes ({row}) VALUES ("
                else:
                    # Join the elements of the row with ," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "GroupsList" in evd_file:
        create_table("GroupsList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields along with the / from Internal/External
                    row = [element.replace("<", "").replace(">", "").replace("/", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Groups ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "GroupMembersList" in evd_file:
        create_table("GroupMembersList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields along with the / from Internal/External
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO GroupMembers ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "UsersList" in evd_file:
        create_table("UsersList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields along with the / from Internal/External
                    row = [element.replace("<", "").replace(">", "").replace("/", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Users ({row}) VALUES ("
                else:
                    # If the "ApplicationMetadata" field has XML the content contains double quotes and needs wrapping in single quotes
                    if "<?xml version=" in row[26]:
                        row[26] = f'\'{row[26]}\''
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Remove the double quotes around element 18
                        row = re.sub('"(\'<\?xml version=.*<\/AppMetaData>\')"', '\\1', row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    else:
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "LocationsList" in evd_file:
        create_table("LocationsList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # Wrap the "Delete" header field with [] as it's a reserved word
                    row = re.sub('(Delete),', '[\\1],', row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Locations ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "ConfirmationsList" in evd_file:
        create_table("ConfirmationsList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # Wrap the "Delete" header field with [] as it's a reserved word
                    row = re.sub('(Delete),', '[\\1],', row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Confirmations ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "Italogfile" in evd_file:
        create_table("Italogfile")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields along with the / from Internal/External
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO ITALog ({row}) VALUES ("
                else:
                    row = "','".join(row)
                    # Add single quotes at the start and end
                    row = f"'{row}'"
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "EventsList" in evd_file:
        create_table("EventsList")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO Events ({row}) VALUES ("
                else:
                    if ('EventData' or 'xml') in row[12]:
                        # Replace the \r\n characters and the whitespace between the tags
                        row[12] = row[12].replace("\r", "").replace("\n", "")
                        row[12] = re.sub(">\s+?<", "><", row[12])
                    # Join the elements of the row with ',' between them
                    row = "','".join(row)
                    # Add single quotes at the start and end
                    row = f"'{row}'"
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "ObjectProperties" in evd_file:
        create_table("ObjectProperties")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO ObjectProperties ({row}) VALUES ("
                else:
                    # If the "ObjectPropertyValue" field has a complex value, the content contains double quotes and needs wrapping in single quotes
                    if '"' in row[4]:
                        row[4] = f'\'{row[4]}\''
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Remove the double quotes around element 5
                        row = re.sub('"(\'.*\')"', '\\1', row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    else:
                        # Join the elements of the row with "," between them
                        row = '","'.join(row)
                        # Add double quotes at the start and end
                        row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
    elif "MasterPolicySettings" in evd_file:
        create_table("MasterPolicySettings")
        print(f'Processing file {evd_file}')
        with open(evd_files_location + evd_file, newline='\n') as csvfile:
            rowcount = sum(1 for line in csvfile)
            csvfile.seek(0)
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            count = 0
            for row in tqdm(reader, total=rowcount, desc=evd_file):
                count += 1
                if count == 1:
                    # Replace the < and > chars from each of the header fields
                    row = [element.replace("<", "").replace(">", "") for element in row]
                    # Join the header back together
                    row = ','.join(row)
                    # build the base SQL
                    base_sql = f"INSERT INTO MasterPolicySettings ({row}) VALUES ("
                else:
                    # Join the elements of the row with "," between them
                    row = '","'.join(row)
                    # Add double quotes at the start and end
                    row = f'"{row}"'
                    # Add the resulting row to the base SQL and add a ) at the end
                    sql = f"{base_sql}{row})"
                    try:
                        cursor.execute(sql)
                        evd_mem_db.commit()
                    except Error as e:
                        print(f'Exception on insert from {evd_file} with line: {count}')
                        print(sql)
                        print(e)
# Copy the DB from memory to the file system
evd_file_db = connect(f'{evd_files_location}pyEVD.db')
evd_mem_db.backup(evd_file_db)
evd_mem_db.close()
evd_file_db.close()
print(f'Import of EVD data took {datetime.datetime.now() - start} seconds to run')

