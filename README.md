# pyEVD
<h3>Import of CyberArk EVD exported data to a SQLite database using Python</h3>
CyberArk provide, out of the box, the script and command file to create the database structure and import the data using bcp and MSSQL. This script does the same thing but without the MSSQL server OS, hardware, licenses and other complications!
</br></br>
This simple script will scan the <strong>evd_files_location</strong> variable as a directory looking for reports partially named:-
<li>FilesList</li>
<li>LogList</li>
<li>OwnersList</li>
<li>RequestsList</li>
<li>SafesList</li>
<li>GroupsList</li>
<li>GroupMembersList</li>
<li>UsersList</li>
<li>LocationsList</li>
<li>ConfirmationsList</li>
<li>Italogfile</li>
<li>EventsList</li>
<li>ObjectProperties</li>
<li>MasterPolicySettings</li>
</br>
If, in your EVD export, you append a datetime stamp as I do, then as long as "FilesList" (for example) stil appears in the filename somewhere it will be found.
</br></br>
Change the <strong>evd_files_location</strong> variable in the script to your value or create a <strong>C:\EVD</strong> directory and put your EVD export files in there.
</br></br>
The script uses <strong>tqdm</strong> <a href="https://github.com/tqdm/tqdm">https://github.com/tqdm/tqdm</a> as a progress bar so you will need to have that module available before running the script.
</br></br>
The output database pyEVD.db is saved the location of the EVD export files themselves, default <strong>C:\EVD</strong>
