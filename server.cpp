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

	void Server::Timestep(){
	}

	void Server::Receive(AppendEntriesRPC rpc){
  	}

	void Server::Receive(AppendEntriesReply reply){
	}

  	void Server::Receive(RequestVoteRPC rpc){
  	}

	void Server::Receive(RequestVoteReply reply){
  	}

  	void Server::Receive(Log log){
		// receive client request
  		if( state == State::LEADER ){
  			log.term = current_term;
  			logs.push_back(log);
  		}
  	}

	void Server::ApplyLog(){
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
