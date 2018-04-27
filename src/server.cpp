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

		vote_granted.clear();
		for(int i = 0 ; i <= cluster_size; i++) {
			vote_granted.push_back(false);
		}
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

	void Server::sendAppendEntriesReply(AppendEntriesRPC rpc, bool success) {
		AppendEntriesReply aer;
		aer.from_id = server_index;
		aer.request = rpc;
		aer.success = success;

		//kirim reply
		sender.Send(rpc.leader_id , aer);
	}

	void Server::sendRequestVoteReply(RequestVoteRPC rpc, bool voted) {
		RequestVoteReply rvr;
		rvr.from_id = server_index;
		rvr.request = rpc;
		rvr.vote_granted = voted;

		//kirim reply
		sender.Send(rpc.candidate_id, rvr);
	}

	//fungsi helper untuk commit
	void Server::leader_commit() {
		//jika leader
		if (state == State::LEADER) {
			int commit_max = -1;

			//untuk tiap log yang ada pada leader
			for(int lg = 0; lg < logs.size(); lg++) {

				//hitung jumlah node yang memiliki log ini
				int count_log_available = 0;
				for (int i = 1; i <= cluster_size; i++) {
					if (match_index[i] >= lg) {
						count_log_available += 1;
					} 
				}

				//jika log dimiliki oleh mayoritas
				if (2 * count_log_available > cluster_size) {
					commit_max = lg;
				}
			}
			//commit index menjadi log dengan index tertinggi
			//yang dimiliki mayoritas server
			commit_index = commit_max;
			ApplyLog();
		}
	}

	void Server::Timestep(){
		//check log to commit in leader server
		leader_commit();

		if (time_to_timeout == 0) {
			if (state == State::LEADER) {
				//server leader time_to_timeout nya 3
				time_to_timeout = 3;
				//kirim heartbeat ke node-node lainnya
				for(int i = 1; i <= cluster_size; i++) {
					if (i != server_index) {
						//heartbeat
						AppendEntriesRPC rpc;
						rpc.term = current_term;
						rpc.leader_id = server_index;
						rpc.leader_commit_index = commit_index;
						rpc.prev_log_index = next_index[i] - 1;

						if(logs.size() > 0)
							rpc.prev_log_term = logs[rpc.prev_log_index].term;
						else 
							rpc.prev_log_term = -1;

						//isi logs yang diperlukan 
						rpc.logs.clear();
						if (next_index[i] < logs.size())
							rpc.logs.push_back(logs[next_index[i]]);

						//send the heartbeat
						sender.Send(i, rpc);
					}
				}
			} else if (state == State::FOLLOWER || state == State::CANDIDATE) {
				//ganti follower jadi candidate
				//start election
				time_to_timeout = 5;
				current_term += 1;
				state = State::CANDIDATE;
				voted_for = server_index;

				//give request vote to all node
				for(int i = 1; i <= cluster_size; i++) {
					if (i != server_index) {
						//create request vote objects
						RequestVoteRPC rpc;
						rpc.term = current_term;
						rpc.candidate_id = server_index;
						rpc.last_log_index = logs.size() - 1;

						if (logs.size() > 0)
							rpc.last_log_term = logs[logs.size() - 1].term;
						else
							rpc.last_log_term = 1;

						//send the request vote
						sender.Send(i, rpc);
					}
				}

				//inisiasi untuk vector vote_granted
				for(int i = 0; i <= cluster_size; i++) {
					vote_granted[i] = false;
				}
				vote_granted[server_index] = true;
			}
		} else if (time_to_timeout != 0) {
			//satu langkah menuju timeout
			time_to_timeout--;
		}
	}

	void Server::Receive(AppendEntriesRPC rpc){
		//jika node merupakan follower atau candidate
		if (state == State::FOLLOWER || state == State::CANDIDATE) {

			//update variable yg perlu di update
			if (rpc.term < current_term) {
				//term dari heartbeat kurang dari term current server
				sendAppendEntriesReply(rpc, false);
			} else { 
				//candidate jika menerma rpc dengan term >= term dia
				//akan berubah jadi follower
				if (state == State::CANDIDATE) {
					state = State::FOLLOWER;
				}

				//reset stat
				time_to_timeout = 5;
				leader = rpc.leader_id;
				commit_index = std::min(rpc.leader_commit_index, (int) logs.size() - 1);

				//jika commit index ketinggalan
				//apply log hingga commit index
				if (commit_index > last_applied) {
					ApplyLog();
				}

				//kalau ganti term, voted_for jadi -1 lagi
				if (current_term < rpc.term) {
					voted_for = -1;
				}	

				current_term = rpc.term;

				//tandanya sudah paling rendah, tidak ada yang sama
				//langsung isi dengan log yang datang
				if (rpc.prev_log_index == -1) {
					logs.clear();
					for(int i = 0; i < rpc.logs.size(); i++) {
						logs.push_back(rpc.logs[i]);
					}
					sendAppendEntriesReply(rpc, true);
				}
				else if (rpc.prev_log_index >= logs.size()){
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
  	}

	void Server::Receive(AppendEntriesReply reply){
		if(state == State::LEADER) {
			if (reply.success) {
				if (next_index[reply.from_id] < logs.size())
					next_index[reply.from_id] += 1;
				match_index[reply.from_id] = reply.request.prev_log_index + reply.request.logs.size();
			} else {
				next_index[reply.from_id] -= 1;
			}
		}
	}

  	void Server::Receive(RequestVoteRPC rpc){
		if (current_term > rpc.term) {
			sendRequestVoteReply(rpc, false);			
		} else if (current_term == rpc.term) {
			//jika leader, tolak request vote yg term sama
			if (state == State::LEADER) {
				sendRequestVoteReply(rpc, false);
			} else {
				if (voted_for == -1 || voted_for == rpc.candidate_id) {
					//voted for menjadi candidate id
					voted_for = rpc.candidate_id;
					time_to_timeout = 5;
					sendRequestVoteReply(rpc, true);
				}
			}
		} else if (current_term < rpc.term) {
			//rubah leader jadi follower
			//jika term kurang dari rpc.term
			if (state == State::LEADER) {
				state = State::FOLLOWER;
			} 

			time_to_timeout = 5;

			//voted for menjadi candidate id
			voted_for = rpc.candidate_id;
			current_term = rpc.term;
			sendRequestVoteReply(rpc, true);
		}
  	}

	void Server::Receive(RequestVoteReply reply){
		if (state == State::CANDIDATE) {
			if (reply.vote_granted) {
				vote_granted[reply.from_id] = true;
			}
			
			//count the vote
			int count_vote = 0;
			for (int i = 1; i <= cluster_size; i++) {
				if(vote_granted[i]) {
					count_vote += 1;
				}
			}

			//if has majority vote
			if (2 * count_vote > cluster_size) {
				//yeay kepilih jadi leader
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
				  << "logs:" << log_str
				  << "timeout" << s.time_to_timeout;
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
				ss << match_index[i] << " ";
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