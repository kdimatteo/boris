import urllib2
from BeautifulSoup import *
from urlparse import urljoin
from pysqlite2 import dbapi2 as sqlite
import sys

import nltk
from nltk.collocations import *


# Create a list of words to ignore
ignorewords={'the':1,'of':1,'to':1,'and':1,'a':1,'in':1,'is':1,'it':1}


class Crawler:
  # Initialize the crawler with the name of database
  def __init__(self, dbname):
    self.con=sqlite.connect("data/" + dbname)
  
  def __del__(self):
    self.con.close()

  def dbcommit(self):
    self.con.commit()

  # Auxilliary function for getting an entry id and adding 
  # it if it's not present
  def getentryid(self, table, field, value, createnew=True):
    cur=self.con.execute(
    "select rowid from %s where %s='%s'" % (table,field,value))
    res=cur.fetchone()
    if res==None:
      cur=self.con.execute(
      "insert into %s (%s) values ('%s')" % (table,field,value))
      return cur.lastrowid
    else:
      return res[0] 


  # Index an individual page
  def addtoindex(self, url, soup):
    if self.isindexed(url): return
    print 'Indexing ' + url
  
    # Get the individual words
    text = self.gettextonly(soup)
    words = self.separatewords(text)
    
    # Get the URL id
    urlid = self.getentryid('urllist', 'url', url)

    bigram_measures = nltk.collocations.BigramAssocMeasures()

    # change this to read in your data
    #finder = BigramCollocationFinder.from_words(nltk.corpus.genesis.words('english-web.txt'))
    finder = BigramCollocationFinder.from_words(words)

    # only bigrams that appear 3+ times
    finder.apply_freq_filter(3) 

    # return the 5 n-grams with the highest PMI
    tags = finder.nbest(bigram_measures.pmi, 5)  


    try:
      c = self.con.execute("insert into wordbag(url, words, tags) values (?, ?, ?)" , (url, text, tags))
    except:
      print "==> bogus wordbag insert"

    # Link each word to this url
    for i in range(len(words)):
      word = words[i]
      if word in ignorewords: continue
      wordid = self.getentryid('wordlist', 'word', word)
      self.con.execute("insert into wordlocation(urlid, wordid, location) values (%d, %d, %d)" % (urlid, wordid, i))

  
  # Extract the text from an HTML page (no tags)
  def gettextonly(self, soup):
    v = soup.string
    if v == None: #or Null   
      c = soup.contents
      resulttext = ''
      for t in c:
        subtext = self.gettextonly(t)
        resulttext += subtext + '\n'
      return resulttext
    else:
      return v.strip()

  # Seperate the words by any non-whitespace character
  def separatewords(self, text):
    splitter=re.compile('\\W*')
    return [s.lower() for s in splitter.split(text) if s!='']

    
  # Return true if this url is already indexed
  def isindexed(self, url):
    c = self.con.execute("select * from urllist where url = '%s' " % url)
    d = c.fetchone()
    if d != None:
      print "skipping duplicate url: " %  url
      return True
    else:
      return False


  '''
  # Add a link between two pages
  def addlinkref(self,urlFrom,urlTo,linkText):
    words=self.separateWords(linkText)
    fromid=self.getentryid('urllist','url',urlFrom)
    toid=self.getentryid('urllist','url',urlTo)
    if fromid==toid: return
    cur=self.con.execute("insert into link(fromid,toid) values (%d,%d)" % (fromid,toid))
    linkid=cur.lastrowid
    for word in words:
      if word in ignorewords: continue
      wordid=self.getentryid('wordlist','word',word)
      self.con.execute("insert into linkwords(linkid,wordid) values (%d,%d)" % (linkid,wordid))
  '''
  
  # Starting with a list of pages, do a breadth
  # first search to the given depth, indexing pages
  # as we go
  def crawl(self, pages, depth=2):
    for i in range(depth):
      newpages={}
      for page in pages:
        try:
          c=urllib2.urlopen(page)
        
        except:
          print "Could not open %s" % page
          continue

        try:
          soup = BeautifulSoup(c.read())
          self.addtoindex(page, soup)

          # recursion into sub pages
          '''
          links = soup('a')

          for link in links:
            if ('href' in dict(link.attrs)):

              url = urljoin(page, link['href'])
              #if url.find("'") != -1: continue
              
              url = url.split('#')[0]  # remove location portion
              if url[0:4] == 'http' and not self.isindexed(url):
                newpages[url] = 1
              
              #linkText = self.gettextonly(link)

              #print linkText
              #self.addlinkref(page, url, linkText)
          '''
          self.dbcommit()
        except:
          print "Could not parse page %s" % page

      pages = newpages

  
  # Create the database tables
  def createindextables(self): 
    self.con.execute('create table urllist(url)')
    self.con.execute('create table wordlist(word)')
    self.con.execute('create table wordlocation(urlid, wordid, location)')
    self.con.execute('create table link(fromid integer,toid integer)')
    self.con.execute('create table linkwords(wordid,linkid)')
    self.con.execute('create table wordbag(url, words, tags)')
    #self.con.execute('create index bagid on wordbag(url)')
    self.con.execute('create index wordidx on wordlist(word)')
    self.con.execute('create index urlidx on urllist(url)')
    self.con.execute('create index wordurlidx on wordlocation(wordid)')
    self.con.execute('create index urltoidx on link(toid)')
    self.con.execute('create index urlfromidx on link(fromid)')
    self.dbcommit()
  """
  def calculatepagerank(self,iterations=20):
    # clear out the current page rank tables
    self.con.execute('drop table if exists pagerank')
    self.con.execute('create table pagerank(urlid primary key,score)')
    
    # initialize every url with a page rank of 1
    for (urlid,) in self.con.execute('select rowid from urllist'):
      self.con.execute('insert into pagerank(urlid,score) values (%d,1.0)' % urlid)
    self.dbcommit()
    
    for i in range(iterations):
      print "Iteration %d" % (i)
      for (urlid,) in self.con.execute('select rowid from urllist'):
        pr=0.15
        
        # Loop through all the pages that link to this one
        for (linker,) in self.con.execute(
        'select distinct fromid from link where toid=%d' % urlid):
          # Get the page rank of the linker
          linkingpr=self.con.execute(
          'select score from pagerank where urlid=%d' % linker).fetchone()[0]

          # Get the total number of links from the linker
          linkingcount=self.con.execute(
          'select count(*) from link where fromid=%d' % linker).fetchone()[0]
          pr+=0.85*(linkingpr/linkingcount)
        self.con.execute(
        'update pagerank set score=%f where urlid=%d' % (pr,urlid))
      self.dbcommit()
  """



if __name__ == "__main__":
  o = Crawler("test-db")
  o.createindextables()
  start_url = ['http://www.cnet.com']
  o.crawl(start_url)

