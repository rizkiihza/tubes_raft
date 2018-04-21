#include <server.hpp>
#include <sstream>
#include <iostream>
namespace raft {
	Server::Server(){
		cluster_size = 0;
		server_index = 0;
	    last_applied = 0;
	    commit_index = 0;
	    data = 0;
	    time_to_timeout = 5;
	    voted_for = -1;
	    current_term = 1;
	    server_index = 0;
	    cluster_size = 0;
	    state = State::FOLLOWER;
	}

	Server::Server(int cluster_size_, int server_index_, Sender sender_) : next_index(cluster_size_+1), match_index(cluster_size_+1), sender(sender_) {
		cluster_size = 0;
		server_index = 0;
	    last_applied = 0;
	    commit_index = 0;
	    data = 0;
	    time_to_timeout = 5;
	    voted_for = -1;
	    current_term = 1;
		cluster_size = cluster_size_;
		server_index = server_index_;
		state = State::FOLLOWER;
	}

	void Server::Crash(){
		// reset state
		voted_for = -1;
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
		std::string state_str = s.GetRoleString();


		std::string log_str = s.GetLogString();

		os << "S" << s.server_index << " "
				  << state_str << " "
				  << "term:" << s.current_term << " " 
				  << "voted_for:" << s.voted_for << " "
				  << "commit_index:" << s.commit_index << " "
				  << "data:" << s.data << " "
				  << "logs:" << log_str;
		return os;
	}

	
	std::string Server::GetLeaderStateString() const {
		std::stringstream ss;

		ss << "L" << server_index << " ";
		ss << "next_index[";
		for( int i = 1; i <= cluster_size; ++ i ){
			if( i == server_index )
				ss << "X ";
			else
				ss << next_index[i] << " ";
		}
		ss << "] ";

		ss << "match_index[";
		for( int i = 1; i <= cluster_size; ++ i ){
			if( i == server_index )
				ss << "X ";
			else
				ss << next_index[i] << " ";
		}
		ss << "]";
		return ss.str();
	}

	std::string Server::GetRoleString() const {
		std::string state_str = "";

		if( state == State::FOLLOWER ) state_str = "follower";
		else if( state == State::LEADER ) state_str = "leader";
		else if( state == State::CANDIDATE ) state_str = "candidate";

		return state_str;
	}

	std::string Server::GetLogString() const {
		std::stringstream log_str; 
		log_str << "T0[0";

		int cterm = 0;
		for( int i = 0; i < logs.size(); ++ i ){
			if( cterm != logs[i].term ){
				log_str << "] T" << logs[i].term << "[";
				cterm = logs[i].term;
			}

			if( logs[i].operation == Operation::MULTIPLY ){
				log_str << "*" << logs[i].payload;
			} else if( logs[i].operation == Operation::ADD ){
				log_str << "+" << logs[i].payload;
			} else if( logs[i].operation == Operation::SUBTRACT ){
				log_str << "-" << logs[i].payload;
			} else if( logs[i].operation == Operation::REPLACE ){
				log_str << "||" << logs[i].payload;
			}
		}
		log_str << "]";

		return log_str.str();
	}

	int Server::GetData() const {
		return data;
	}

	int Server::GetCommitIndex() const {
		return commit_index;
	}
}