import pandas
import os
import sqlite3
import pickle

conn = sqlite3.connect("annotationhub.sqlite3")
cur = conn.cursor()
cur.execute("select * from resources r JOIN input_sources inp_src ON r.id = inp_src.resource_id;")
results = cur.fetchall()
pd = pandas.DataFrame(results, columns = ["id", "ah_id", "title", "dataprovider", "species", "taxonomyid", "genome", 
                                        "description", "coordinate_1_based", "maintainer", "status_id",
                                        "location_prefix_id", "recipe_id", "rdatadateadded", "rdatadateremoved",
                                        "record_id", "preparerclass", "id", "sourcesize", "sourceurl", "sourceversion",
                                        "sourcemd5", "sourcelastmodifieddate", "resource_id", "source_type"])

# most files in roadmap data repository are bigwigs
roadmap = pd.query('dataprovider=="BroadInstitute" and genome=="hg19" and (source_type=="BigWig" or source_type=="BigBed" or source_type=="tabix")')
roadmap

roadmap.to_csv(os.getcwd() + "/roadmap_all.csv")
rfile = open(os.getcwd() + "/roadmap.pickle", "wb")

pickle.dump(roadmap, rfile)
# print(roadmap.shape)