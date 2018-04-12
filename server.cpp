#include <server.hpp>
#include <sstream>
#include <iostream>
namespace raft {
	Server::Server(){
		cluster_size = 0;
		server_index = 0;
	}

	Server::Server(int cluster_size_, int server_index_, Sender sender_) : next_index(cluster_size_+1), match_index(cluster_size_+1), sender(sender_) {
		current_term = 1;
		voted_for = -1;
		time_to_timeout = 5;
		data = 0;
		cluster_size = cluster_size_;
		server_index = server_index_;
		state = State::FOLLOWER;
	}

	void Server::Crash(){
		// reset state
		time_to_timeout = 5;
		state = State::FOLLOWER;
	}

	void Server::SetTimeout(int ttt){
		time_to_timeout = ttt;
	}

	//fungsi helper untuk mengirim append entries reply
	void Server::sendAppendEntriesReply(AppendEntriesRPC rpc, bool success) {
		AppendEntriesReply aer;
		aer.from_id = server_index;
		aer.request = rpc;
		aer.success = success;

		//kirim reply
		sender.Send(rpc.leader_id , aer);
	}

	//fungsi helper untuk mengirim request vote reply
	void Server::sendRequestVoteReply(RequestVoteRPC rpc, bool voted) {
		RequestVoteReply rvr;
		rvr.from_id = server_index;
		rvr.request = rpc;
		rvr.vote_granted = voted;

		//kirim reply
		sender.Send(rpc.candidate_id, rvr);
	}

	//BELUM KELAR
	void Server::Timestep(){
		if (time_to_timeout == 0) {
			if (state == State::LEADER) {
				//server leader time_to_timeout nya 3
				time_to_timeout = 3;

				//send heartbeat to all other nodes
				for(int i = 1; i <= cluster_size && i != server_index; i++) {
					//create heartbeat rpc object
					AppendEntriesRPC rpc;
					rpc.term = current_term;
					rpc.leader_id = server_index;
					rpc.leader_commit_index = commit_index;
					rpc.prev_log_index = next_index[i] - 1;
					rpc.prev_log_term = logs[rpc.prev_log_index].term;

					//isi logs yang diperlukan 
					rpc.logs.clear();
					for(int j = next_index[i]; j < logs.size(); j++) {
						rpc.logs.push_back(logs[j]);
					}

					//send the heartbeat
					sender.Send(i, rpc);
				}
			} else if (state == State::FOLLOWER) {
				//ganti follower jadi candidate
				//start election
				time_to_timeout = 5;
				state = State::CANDIDATE;

				//give request vote to all node
				for(int i = 1; i <= cluster_size && i != server_index; i++) {
					//create request vote objects
					RequestVoteRPC rpc;
					rpc.term = current_term;
					rpc.candidate_id = server_index;
					rpc.last_log_index = logs.size() - 1;
					rpc.last_log_term = logs[logs.size() - 1].term;

					//send the request vote
					sender.Send(i, rpc);
				}
			}
		} else {
			//satu langkah menuju timeout
			time_to_timeout--;
		}
	}

	void Server::Receive(AppendEntriesRPC rpc){
		//jika node merupakan follower atau candidate
		if (state == State::FOLLOWER || state == State::CANDIDATE) {

			//candidate jika menerma rpc akan berubah jadi follower
			if (state == State::CANDIDATE) {
				state = State::FOLLOWER;
			}

			//update variable yg perlu di update
			time_to_timeout = 5;
			leader = rpc.leader_id;

			if (rpc.term < current_term) {
				//term dari heartbeat kurang dari term current server

				sendAppendEntriesReply(rpc, false);
			} else if (rpc.prev_log_index >= logs.size()){
				//current server tidak punya log dengan index prev_log_index

				sendAppendEntriesReply(rpc, false);
			} else if (logs[rpc.prev_log_index].term != rpc.prev_log_term) {
				//term dari log dengan index prev_log_index tidak sama dengan rpc.prev_log_term

				sendAppendEntriesReply(rpc, false);
			} else if(logs[rpc.prev_log_index].term == rpc.prev_log_term){

				//hapus logs yg conflict sama logs yang dikirim
				for(int i = rpc.prev_log_index + 1; i < logs.size(); i++) {
					logs.erase(logs.begin() + i);
				}

				//tambahkan log yang index nya lebih dari logs.size()
				for(int i = 0; i <  rpc.logs.size(); i++) {
					logs.push_back(rpc.logs[i]);
				}

				sendAppendEntriesReply(rpc, true);
			}
		} 
  	}

	void Server::Receive(AppendEntriesReply reply){

	}

  	void Server::Receive(RequestVoteRPC rpc){
		  
  	}

	void Server::Receive(RequestVoteReply reply){
  	}

  	void Server::Receive(Log mLog){
		// receive client request
  		if( state == State::LEADER ){
  			mLog.term = current_term;
  			logs.push_back(mLog);
  		} 
  	}

	void Server::ApplyLog(){
		//commit log yang belum dicommit tp bisa dicommit
		//dari last_applid + 1 hingga commit_index
		for(int i = last_applied + 1; i <= commit_index; i++) {
			Log current_log = logs[i];

			//if block untuk semua jenis operasi
			if (current_log.operation == Operation::ADD) {
				data = data + current_log.payload;
			} else if (current_log.operation == Operation::SUBTRACT) {
				data = data - current_log.payload;
			} else if (current_log.operation == Operation::MULTIPLY) {
				data = data * current_log.payload;
			} else if (current_log.operation == Operation::REPLACE) {
				data = current_log.payload;
			} 
		}
	}

	std::ostream & operator<<(std::ostream &os, const Server& s){
		std::string state_str = "";

		if( s.state == State::FOLLOWER ) state_str = "follower";
		else if( s.state == State::LEADER ) state_str = "leader";
		else if( s.state == State::CANDIDATE ) state_str = "candidate";

		std::stringstream log_str; 
		log_str << "T0[0";

		int cterm = 0;
		for( int i = 0; i < s.logs.size(); ++ i ){
			if( cterm != s.logs[i].term ){
				log_str << "]\nT" << s.logs[i].term << "[";
				cterm = s.logs[i].term;
			}

			if( s.logs[i].operation == Operation::MULTIPLY ){
				log_str << "*" << s.logs[i].payload;
			} else if( s.logs[i].operation == Operation::ADD ){
				log_str << "+" << s.logs[i].payload;
			} else if( s.logs[i].operation == Operation::SUBTRACT ){
				log_str << "-" << s.logs[i].payload;
			} else if( s.logs[i].operation == Operation::REPLACE ){
				log_str << "||" << s.logs[i].payload;
			}
		}
		log_str << "]\n";


		os << "<<Server>>\n" 
				  << "server_index:" << s.server_index << "\n"
				  << "state:" << state_str << "\n"
				  << "term:" << s.current_term << "\n" 
				  << "voted_for:" << s.voted_for << "\n"
				  << "commit_index:" << s.commit_index << "\n"
				  << "data:" << s.data << "\n"
				  << "logs:\n" << log_str.str();
		return os;
	}

	
	std::string Server::GetLeaderStateString(){
		std::stringstream ss;

		ss << "<<LeaderServer>>\n";
		ss << "server_index:" << server_index << "\n";
		ss << "next_index:[";
		for( int i = 1; i <= cluster_size; ++ i ){
			ss << next_index[i] << " ";
		}
		ss << "]\n";

		ss << "match_index:[";
		for( int i = 1; i <= cluster_size; ++ i ){
			ss << next_index[i] << " ";
		}
		ss << "]";
		return ss.str();
	}
}
