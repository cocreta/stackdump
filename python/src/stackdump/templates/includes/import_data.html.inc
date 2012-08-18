<h2>Get the StackExchange data dump</h2>
<p>
    StackExchange data dumps are distributed using BitTorrent. You
    will need a BitTorrent client like
    <a href="http://www.utorrent.com">uTorrent</a> to download it.
</p>
<p>
    The data dumps can be downloaded from 
    <a href="http://www.clearbits.net/creators/146-stack-exchange-data-dump">http://www.clearbits.net/creators/146-stack-exchange-data-dump</a>.
</p>

<h2>Extract the dump</h2>
<p>
    Once downloaded, you will end up with a directory with another
    directory within it named <em>Content</em>. Inside that
    <em>Content</em> directory contains the data dumps of each site
    compressed in a <a href="http://www.7-zip.org/">7-zip</a> file.
</p>
<p>
    For each of the sites you wish to import into Stackdump, extract
    the compressed file to a temporary location (each compressed
    file contains another directory inside it with the actual data,
    so you can just extract each file into the same temporary
    location).
</p>

<h2>Import them into Stackdump</h2>
<p>
    This process can take upwards of 10 hours or more per site depending on
    the size of the dump you're trying to import. StackOverflow will take around
    10 hours, while the smaller ones like android.stackexchange.com take about
    a minute or less.
</p>
<p>
    Before you can import data though, you need to download the
    required metadata so Stackdump can load the dumps properly.
</p>
<ol>
    <li>Fire up a terminal/command prompt and navigate to the directory you extracted Stackdump into.</li>
    <li>
        Execute the following command -
        <pre>./manage.sh download_site_info</pre>
    </li>
</ol>
<p>
    Now that you have the site metadata, you can import the dumps.
    For each dump you wish to import, do the following -
</p>
<ol>
    <li>Fire up a terminal/command prompt and navigate to the directory you extracted Stackdump into.</li>
    <li>Find the directory containing the data dump XML files. This is likely to be a directory inside the temporary location you extracted to earlier. The directory will contain files like <em>posts.xml</em>, <em>users.xml</em> and <em>comments.xml</em>.</li>
    <li>
        Execute the following command, replacing <em>path_to_dir_with_xml</em> with the path from the previous step -
        <pre>./manage.sh import_site path_to_dir_with_xml</pre>
    </li>
</ol>
<p>
    You will most likely have to specify the site's base URL (e.g.
    <em>programmers.stackexchange.com</em>) and the dump date (e.g.
    <em>August 2012</em>) for the import process to have enough information to
    proceed. The command will prompt if this is required.
</p>