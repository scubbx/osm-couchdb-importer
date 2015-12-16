# -*- coding: utf-8 -*-

import xml.parsers.expat
import mpcouch
import bz2
# import eventlet

currentTreeData = [] # just to initialize
#entries = 0
nodes = 0
ways = 0
relations = 0


def elementReader(filename):
    couchPusher = mpcouch.mpcouchPusher("http://gi88.geoinfo.tuwien.ac.at:5984/osmtreesaustria_allhistory",100,threads = False, jobsbuffersizemax = 500)
    oldids = []
    
    def gotCompleteEntry(entry):
        couchPusher.pushData({'data':entry, '_id': entry[0]['version'] + '-' + entry[0]['id']})
        #print(entry)
    
    def start_osm_element(name, attrs):
        global currentTreeData
        if   name == "node":
            '''start collecting information including all sub-keys'''
            currentTreeData = []
            currentTreeData.append(attrs) # the current meta-information
            currentTreeData.append({})    # for the tags
        elif name == "tag":
            '''collect the tag-information'''
            key = attrs[u'k']
            value = attrs[u'v']
            #print key
            #print value
            currentTreeData[1][key] = value
        else:
            print("uncatched element: {}".format(name))
    
    def end_osm_element(name):
        global currentTreeData
        global entries
        global nodes
        global ways
        global relations
        #entries += 1
        #if entries % 1000000 == 0: print("Processed {} XML entries".format(entries))
        
        if name == "node":
            #nodes += 1
            #if nodes % 100000 == 0: print("Processed {} OSM nodes".format(nodes))
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            """
            The following checks if we are required to include this doc in any way.
            This is the case, if it has an id which is contained in the oldversions variabel.
            """
            if currentTreeData[0]['id'] in oldids:
                print("Got old version: {} of {}, adding.".format(len(currentTreeData[0]['version']), currentTreeData[0]['id'] ))
                gotCompleteEntry(currentTreeData)
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            """
            The uncommented following code is there to make sure only trees are kept.
            """
            if 'natural' in currentTreeData[1]:
                if currentTreeData[1]['natural'] == 'tree': # yay, it's a tree !
                    if currentTreeData[0]['version'] != 1:
                        oldids.append(currentTreeData[0]['id'])
                        # print("oldidsremark: {}".format(len(currentTreeData[0]['version'])))
                    gotCompleteEntry(currentTreeData)
            # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            
            #gotCompleteEntry(currentTreeData)
        
        elif name == "way":
            #ways += 1
            #if ways % 100000 == 0: print("Processed {} OSM ways".format(ways))
            
            # !!!!!!!!!!!!!!!!!!!!!! DIRTY HACK HERE !!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!! DO NOT TRY AT HOME !!!!!!!!!!!!!!!!!
            couchPusher.finish()
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            
        elif name == "relation":
            pass
            #relations += 1
            #if relations % 100000 == 0: print("Processed {} OSM relations".format(relations))
    
    def char_osm_data(data):
        pass
    
    osmParser = xml.parsers.expat.ParserCreate()
    osmParser.StartElementHandler = start_osm_element
    osmParser.EndElementHandler = end_osm_element
    #osmParser.CharacterDataHandler = char_osm_data
    
    if filename[-3:] == 'bz2':
        with bz2.open(filename, 'rb') as osmFile:
            print("start parsing")
            osmParser.ParseFile(osmFile)
            print("finished parsing")
    else:
        with open(filename, 'rb') as osmFile:
            print("start parsing")
            osmParser.ParseFile(osmFile)
            print("finished parsing")

    
    couchPusher.finish()

if __name__ == '__main__':
    print("running import")
    elementReader("/datenspeicher/OSM_full_history/austria-history-151207.osh.bz2")
    print("finished import")
