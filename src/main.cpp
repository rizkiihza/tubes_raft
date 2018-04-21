#include <iostream>
#include <string>
#include <vector>
#include <raft_simulation.hpp>

int main(){
	int cluster_size;
	bool running = true;

	std::cin >> cluster_size;

	raft::RaftSimulation simulation(cluster_size);
	
	while(running){
		std::string cmd;
		int server;

		std::cin >> cmd;
		if( cmd == "force_timeout" ){
			std::cin >> server;
			simulation.SetTimeout(server, 0);
		} else if( cmd == "set_timeout" ){
			int ttt;
			std::cin >> server >> ttt;
			simulation.SetTimeout(server, ttt);
		} else if( cmd == "timestep"){
			simulation.Timestep();
		} else if( cmd == "crash" ){
			std::cin >> server;
			simulation.Crash(server);
		} else if( cmd == "start" ){
			std::cin >> server;
			simulation.Start(server);
		} else if( cmd == "restart" ){
			std::cin >> server;
			simulation.Start(server);
		} else if( cmd == "get_state" ){
			std::cin >> server;
			simulation.PrintServerState(server);
		} else if( cmd == "get_role" ){
			std::cin >> server;
			simulation.PrintServerRole(server);
		} else if( cmd == "get_log" ){
			std::cin >> server;
			simulation.PrintServerLog(server);
		} else if( cmd == "get_data" ){
			std::cin >> server;
			simulation.PrintServerData(server);
		} else if( cmd == "get_commit_index" ){
			std::cin >> server;
			simulation.PrintServerCommitIndex(server);
 		} else if( cmd == "get_leader_state" ) {
			std::cin >> server;
			simulation.PrintServerLeaderState(server);
		} else if( cmd == "get_outbox" ) {
			simulation.PrintOutbox();
		} else if( cmd == "store_outbox" ){
			std::string key;
			std::cin >> key;
			simulation.StoreOutbox(key);
		} else if( cmd == "delete_outbox" ){
			simulation.DeleteOutbox();
		} else if( cmd == "retrieve_outbox" ){
			std::string key;
			std::cin >> key;
			simulation.RetrieveOutbox(key);
		} else if( cmd == "perfect_shuffle_outbox" ){

		} else if( cmd == "flush_outbox" ){
			simulation.FlushOutbox();
		} else if( cmd == "client_send_request" ) {
			std::cin >> server;

			raft::Log log;
			std::string opc;
			std::cin >> opc;
			if( opc == "+" )
				log.operation = raft::Operation::ADD;
			else if( opc == "*" )
				log.operation = raft::Operation::MULTIPLY;
			else if( opc == "-" )
				log.operation = raft::Operation::SUBTRACT;
			else if( opc == "||" )
				log.operation = raft::Operation::REPLACE;

			std::cin >> log.payload;
			simulation.ClientSendRequest(server, log);
		} else if( cmd == "simulate" ) {
			int timestep;
			std::cin >> timestep;

			for( int i = 1; i <= timestep; ++ i ){
				simulation.Timestep();
				simulation.FlushOutbox();
			}

		} else if( cmd == "disconnect"){
			int a,b;
			std::cin >> a >> b;

			simulation.Disconnect(a,b);
		} else if( cmd == "connect"){
			int a,b;
			std::cin >> a >> b;

			simulation.Connect(a,b);
		} else if( cmd == "end_simulation" ){
			running = false;
		}
	}

	return 0;
}