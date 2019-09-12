from epivizfileserver.trackhub import TrackHub

th = TrackHub("http://data.nemoarchive.org/nemoHub")
# print(th.hub)
# print(th.genomes)
print(th.mMgr.measurements)
