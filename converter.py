import os
import sqlite3
import json
import pprint

def parse_multipart(filename, db_name = "output.db"):
	"""Open file.
	For each line, take a look at the content.
	If the json does not parse, then move on.
	Otherwise, parse it and make a dictionary.
	With that dictionary, pull out the metadata.  Assign this
	user a user number.  Create the following tables:
	
	users
	-----------
	id, os, etc.
	
	extensions
	-----------
	id
	
	users_extensions
	-----------
	user_id, extension_id
	
	events
	-----------
	user_id, other columns defined by the data in the 'metadata' value.
	
	"""
	
	type_dict = {
		list: "TEXT",
		float: "NUMERIC",
		int: "INT",
		unicode: "TEXT"
	}
	
	f = open(filename, "r")
	uc = 0
	
	if os.path.exists(db_name):
		os.remove(db_name)
	
	db = sqlite3.connect(db_name)
	c = db.cursor()
	
	event_sample_taken = False
	dataset_is_empty = True
	
	c.execute("""CREATE TABLE users(id INT, location TEXT, fx_version TEXT, os TEXT, version TEXT, survey_answers TEXT);""")
	
	extensions_mapping = {}
	ec = 0
	c.execute("""CREATE TABLE extensions(id INT, hash TEXT);""")
	c.execute("""CREATE TABLE user_extensions(user_id INT, extension_id INT, is_enabled INT);""")
	
	acc_mapping = {}
	acount = 0
	c.execute("""CREATE TABLE accessibilities(id INT, name TEXT);""")
	c.execute("""CREATE TABLE user_accessibilities(user_id INT, acc_id INT, value TEXT)""")
	
	for i, line in enumerate(f):
		try:
			user_data = json.loads(line)
		except:
			user_data = None
		if user_data and user_data['data']:
			#p = pprint.PrettyPrinter(indent=4)
			#p.pprint(user_data)
			
			found = True
			
			try:
				user_data = user_data["data"][0]
				metadata = user_data['metadata']
				events = user_data['events']
			except:
				print "user data not right for line %s: %s" % (i, line)
				found = False
			if found:
				
				if i % 1000 == 0:
					print "User %s" % i
				
				event_header = [str(t) for t in metadata['event_headers']]
				
				# from events back out column types.
				
				if not event_sample_taken:
					event_sample = events[0:2]
				
					t1 = [type_dict[type(ev)] for ev in event_sample[0]]
					t2 = [type_dict[type(ev)] for ev in event_sample[1]]
					if t1 == t2:
						# we're good.  Create the event table.
						
						if "timestamp" in event_header:
							t1.append("INT")
							event_header.append("session_id")
						
						table_def = ", " .join([" ".join([ev, t]) for ev, t in zip(event_header, t1)])
						query = """CREATE TABLE events (user_id INT, %s);""" % table_def
						print query
						c.execute(query)
						event_query = "INSERT INTO events VALUES(%s);" % ",".join(["?" for i in range(len(t1) + 1)])
					else:
						print "WTF?", event_sample, t1, t2
						import sys; sys.exit()
					event_sample_taken = True
			
				# User.
			
				location = metadata['location']
				fxVersion = metadata['fxVersion']
				operatingSystem = metadata['operatingSystem']
				tpVersion = metadata['tpVersion']
				surveyAnswers = json.dumps(metadata['surveyAnswers'])
				c.execute("""INSERT INTO users VALUES(?,?,?,?,?,?);""",
					(uc,location,fxVersion,operatingSystem,tpVersion,surveyAnswers))
			
				# Events.
				# TODO: create coherent session ids!
				# first: implement the flow stuff here.
				# next: integrate everything into the table def description.
				if 'timestamp' in event_header:
					session_id = 0
					sessions = []
					current_session = []
					current_event = None
					prev_event = None
					# get the timestamp index.
					tsind = [i for i, t in enumerate(event_header) if t == u'timestamp'][0]
					#print tsind
					for event in events:
						current_event = event
						# get the timestamp.
						cts = current_event[tsind] / 1000.0 / 60
						if prev_event:
							pts = prev_event[tsind] / 1000.0 / 60
							#print cts - pts
							if cts - pts > 30:
								print "made it.", cts-pts
								sessions.append(list(current_session))
								current_session = []
						else:
							pass
						print current_event
						current_session.append(list(current_event))
						prev_event = current_event
					for i, session in enumerate(sessions):
						for event in session:
							print [uc] + event + [i]
							c.execute(event_query, [uc] + event + [i])
				else:
					for event in events:
						c.execute(event_query, [uc] + event)
				#for event in events:
			
				extensions = metadata['extensions']
			
				for ex in extensions:
					eid = ex['id']
					isEnabled = ex['isEnabled']
					if eid not in extensions_mapping:
						extensions_mapping[eid] = ec
						c.execute("""INSERT INTO extensions VALUES(?,?);""",[ec, eid])
						ec += 1
					current_eid = extensions_mapping[eid]
					c.execute("""INSERT INTO user_extensions VALUES(?,?,?);""",[uc, current_eid, isEnabled])
				
				# Accessibilities.
				
				accessibilities = metadata['accessibilities']
				
				for ac in accessibilities:
					name = ac['name']
					value = ac['value']
				
					if name not in acc_mapping: 
						acc_mapping[name] = acount
						c.execute("""INSERT INTO accessibilities VALUES(?,?);""",[acount,name])
						acount += 1
					current_acount = acc_mapping[name]
					c.execute("""INSERT INTO user_accessibilities VALUES(?,?,?);""", [uc, current_acount, value])
			
				uc += 1
				dataset_is_empty = False
	if dataset_is_empty:
		raise ValueError, "The multipart dump was empty.  Check to see if it is."
	else:
		c.execute("""CREATE INDEX event_user_id ON events(user_id);""")
		c.execute("""CREATE INDEX user_user_id ON users(id);""")
		db.commit()

if __name__ == "__main__":
	parse_multipart("beta_test.txt", "bt.db")
	db = sqlite3.connect("bt.db")
	c = db.cursor()
	c.execute("""SELECT * from events LIMIT 10;""")
	print c.fetchall(), "!"