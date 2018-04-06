make :
	g++ -o raft -I . main.cpp log.cpp raft_simulation.cpp sender.cpp server.cpp mail.cpp