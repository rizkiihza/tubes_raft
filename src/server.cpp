#include <server.hpp>
#include <sstream>
#include <iostream>
namespace raft {
	Server::Server(){
		cluster_size = 0;
		server_index = 0;
	    last_applied = -1;
	    commit_index = -1;
	    data = 0;
	    time_to_timeout = 4;
	    voted_for = -1;
	    current_term = 1;
	    server_index = 0;
	    cluster_size = 0;
	    state = State::FOLLOWER;
	}

	Server::Server(int cluster_size_, int server_index_, Sender sender_) : next_index(cluster_size_+1), match_index(cluster_size_+1), sender(sender_) {
		cluster_size = 0;
		server_index = 0;
	    last_applied = -1;
	    commit_index = -1;
	    data = 0;
	    time_to_timeout = 4;
	    voted_for = -1;
	    current_term = 1;
		cluster_size = cluster_size_;
		server_index = server_index_;
		state = State::FOLLOWER;

		vote_granted.clear();
		for(int i = 0 ; i <= cluster_size; i++) {
			vote_granted.push_back(false);
		}
	}

	void Server::Crash(){
		voted_for = -1;
		time_to_timeout = 5;
		state = State::FOLLOWER;
	}

	void Server::SetTimeout(int ttt){
		time_to_timeout = ttt;
	}

	void Server::sendAppendEntriesReply(AppendEntriesRPC rpc, bool success, int term) {
		AppendEntriesReply aer;
		aer.from_id = server_index;
		aer.request = rpc;
		aer.success = success;
		aer.term = term;
		sender.Send(rpc.leader_id , aer);
	}

	void Server::sendRequestVoteReply(RequestVoteRPC rpc, bool voted, int term) {
		RequestVoteReply rvr;
		rvr.from_id = server_index;
		rvr.request = rpc;
		rvr.vote_granted = voted;
		rvr.term = term;

		sender.Send(rpc.candidate_id, rvr);
	}

	void Server::leader_commit() {
		if (state == State::LEADER) {
			int commit_max = -1;

			for(int lg = 0; lg < logs.size(); lg++) {

				int count_log_available = 0;
				for (int i = 1; i <= cluster_size; i++) {
					if (i != server_index) {
						if (match_index[i] >= lg) {
							count_log_available += 1;
						}
					}
				}

				if (2 * (count_log_available + 1) > cluster_size) {
					commit_max = lg;
				}
			}
			if (commit_max > commit_index && logs[commit_max].term == current_term) {
				commit_index = commit_max;
			}
			ApplyLog();
		}
	}

	void Server::Timestep(){
		leader_commit();

		if (time_to_timeout == 0) {
			if (state == State::LEADER) {
				time_to_timeout = 2;
				for(int i = 1; i <= cluster_size; i++) {
					if (i != server_index) {
						AppendEntriesRPC rpc;
						rpc.term = current_term;
						rpc.leader_id = server_index;
						rpc.leader_commit_index = commit_index;
						rpc.prev_log_index = next_index[i] - 1;

						if(logs.size() > 0)
							rpc.prev_log_term = logs[rpc.prev_log_index].term;
						else
							rpc.prev_log_term = -1;

						rpc.logs.clear();
						if (next_index[i] < logs.size())
							rpc.logs.push_back(logs[next_index[i]]);

						sender.Send(i, rpc);
					}
				}
			} else if (state == State::FOLLOWER || state == State::CANDIDATE) {
				time_to_timeout = 4;
				current_term += 1;
				state = State::CANDIDATE;
				voted_for = server_index;

				for(int i = 1; i <= cluster_size; i++) {
					if (i != server_index) {
						RequestVoteRPC rpc;
						rpc.term = current_term;
						rpc.candidate_id = server_index;
						rpc.last_log_index = logs.size() - 1;

						if (logs.size() > 0)
							rpc.last_log_term = logs[logs.size() - 1].term;
						else
							rpc.last_log_term = 1;

						sender.Send(i, rpc);
					}
				}

				for(int i = 0; i <= cluster_size; i++) {
					vote_granted[i] = false;
				}
				vote_granted[server_index] = true;
			}
		} else if (time_to_timeout > 0) {
			time_to_timeout--;
		}
	}

	void Server::Receive(AppendEntriesRPC rpc){
		if (state == State::FOLLOWER || state == State::CANDIDATE) {

			if (rpc.term < current_term) {
				sendAppendEntriesReply(rpc, false, current_term);
			} else {
				if (state == State::CANDIDATE) {
					state = State::FOLLOWER;
				}

				time_to_timeout = 5;
				leader = rpc.leader_id;

				if (current_term < rpc.term) {
					voted_for = -1;
				}

				current_term = rpc.term;

				if (rpc.prev_log_index == -1) {
					logs.clear();
					for(int i = 0; i < rpc.logs.size(); i++) {
						logs.push_back(rpc.logs[i]);
					}
					sendAppendEntriesReply(rpc, true, rpc.term);
				}
				else if (rpc.prev_log_index >= logs.size()){
					sendAppendEntriesReply(rpc, false, rpc.term);
				} else if (logs[rpc.prev_log_index].term != rpc.prev_log_term) {
					sendAppendEntriesReply(rpc, false, rpc.term);
				} else if(logs[rpc.prev_log_index].term == rpc.prev_log_term){
					for(int i = rpc.prev_log_index + 1; i < logs.size(); i++) {
						logs.erase(logs.begin() + i);
					}

					for(int i = 0; i <  rpc.logs.size(); i++) {
						logs.push_back(rpc.logs[i]);
					}

					sendAppendEntriesReply(rpc, true, rpc.term);
				}

				commit_index = std::min(rpc.leader_commit_index, (int) logs.size());

				ApplyLog();
			}
		}
  	}

	void Server::Receive(AppendEntriesReply reply){
		if(reply.term > current_term) {
			state = State::FOLLOWER;
		} else if(state == State::LEADER) {
			if (reply.success) {
				if (next_index[reply.from_id] < logs.size())
					next_index[reply.from_id] += 1;
				match_index[reply.from_id] = reply.request.prev_log_index + reply.request.logs.size();
			} else {
					next_index[reply.from_id] -= 1;
			}
		}
		leader_commit();
	}

	void Server::Receive(RequestVoteRPC rpc){
		if (current_term > rpc.term) {
			int diff_term = current_term;
			sendRequestVoteReply(rpc, false, diff_term);
		} else if (current_term == rpc.term) {
			if (state == State::LEADER) {
				sendRequestVoteReply(rpc, false, rpc.term);
			} else {
				if (voted_for == -1 || voted_for == rpc.candidate_id) {
					if (state == State::CANDIDATE) {
						state = State::FOLLOWER;
					}
					if (logs.size() > 0) {
						int lastLogIdx = logs.size() - 1;
						int lastLogTerm = logs[lastLogIdx].term;
						if (rpc.last_log_term >= lastLogTerm) {
							current_term = rpc.term;
							voted_for = rpc.candidate_id;
							time_to_timeout = 5;
							sendRequestVoteReply(rpc, true, current_term);
						}
						else {
							sendRequestVoteReply(rpc, false, current_term);
						}
					} else {
							sendRequestVoteReply(rpc, true, current_term);
					}
				}

			}
		} else if (current_term < rpc.term) {
			if (state == State::LEADER) {
				state = State::FOLLOWER;
			}

			if (state == State::CANDIDATE) {
				state = State::FOLLOWER;
			}

			time_to_timeout = 5;

			voted_for = rpc.candidate_id;
			current_term = rpc.term;

			if (logs.size() > 0) {
				int lastLogIdx = logs.size() - 1;
				int lastLogTerm = logs[lastLogIdx].term;
				if (rpc.last_log_term >= lastLogTerm) {
					current_term = rpc.term;
					voted_for = rpc.candidate_id;
					time_to_timeout = 5;
					sendRequestVoteReply(rpc, true, current_term);
				} else {
					sendRequestVoteReply(rpc, false, current_term);
				}
			}
			else {
				sendRequestVoteReply(rpc, true, current_term);
			}
		}
	}

	void Server::Receive(RequestVoteReply reply){
		if (reply.term > current_term) {
			state = State::FOLLOWER;
			current_term = reply.term;
		}

		if (state == State::CANDIDATE) {
			if (reply.vote_granted) {
				vote_granted[reply.from_id] = true;
			}

			int count_vote = 0;
			for (int i = 1; i <= cluster_size; i++) {
				if(vote_granted[i]) {
					count_vote += 1;
				}
			}
			if (2 * count_vote > cluster_size) {
				state = State::LEADER;
				time_to_timeout = 0;
				leader = server_index;

				for(int i = 1; i <= cluster_size; i++) {
					next_index[i] = logs.size();
					match_index[i] = -1;
				}
			}
		}
  	}

  	void Server::Receive(Log log){
  		if( state == State::LEADER ){
  			log.term = current_term;
  			logs.push_back(log);
  		}
  	}

	void Server::ApplyLog(){

		int starting = last_applied;
		int last_index = 0;
		data = 0;
		for(int i = 0; i <= commit_index && i < logs.size(); i++) {
			Log current_log = logs[i];

			if (current_log.operation == Operation::ADD) {
				data = data + current_log.payload;
			} else if (current_log.operation == Operation::SUBTRACT) {
				data = data - current_log.payload;
			} else if (current_log.operation == Operation::MULTIPLY) {
				data = data * current_log.payload;
			} else if (current_log.operation == Operation::REPLACE) {
				data = current_log.payload;
			}
			last_index = i;
		}
		last_applied = last_index - 1;
	}


	std::ostream & operator<<(std::ostream &os, const Server& s){
		std::string state_str = s.GetRoleString();


		std::string log_str = s.GetLogString();

		os << "S" << s.server_index << " "
				  << state_str << " "
				  << "term:" << s.current_term << " "
				  << "voted_for:" << s.voted_for << " "
				  << "commit_index:" << s.commit_index + 1<< " "
				  << "data:" << s.data << " "
				  << "logs:" << log_str << " ";
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
				ss << next_index[i] + 1 << " ";
		}
		ss << "] ";

		ss << "match_index[";
		for( int i = 1; i <= cluster_size; ++ i ){
			if( i == server_index )
				ss << "X ";
			else
				ss << match_index[i] + 1<< " ";
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
		return commit_index + 1;
	}
}
