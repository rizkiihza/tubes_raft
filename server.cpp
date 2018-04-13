#include <server.hpp>
#include <sstream>
#include <iostream>
#include <algorithm>
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

		last_applied = -1;
		commit_index = -1;

		vote_granted.clear();
		for(int i = 0 ; i <= cluster_size; i++) {
			vote_granted.push_back(false);
		}
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


	//klo sukses : match_index jadi log terakhir yang dikirim
	//klo gagal : next_index dikurangi satu
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
					sendRequestVoteReply(rpc, true);
				}
			}
		} else if (current_term < rpc.term) {
			//rubah leader jadi follower
			//jika term kurang dari rpc.term
			if (state == State::LEADER) {
				state = State::FOLLOWER;
			} 

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

  	void Server::Receive(Log mLog){
		// receive client request
  		if( state == State::LEADER ){
			mLog.term = current_term;
			logs.push_back(mLog);
			next_index[server_index] = logs.size();
			match_index[server_index] = logs.size() - 1;
  		} 
  	}

	void Server::ApplyLog(){
		//commit log yang belum dicommit tp bisa dicommit
		//dari last_applied + 1 hingga commit_index

		int starting = last_applied;
		for(int i = starting+1; i <= commit_index && i < logs.size(); i++) {
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
			last_applied += 1; 
		}

		//ganti last applied menjadi index yang terakhir di commit di server ini
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
				  << "logs:\n" << log_str.str()

				  //extra info
				  //hapus sebelum kumpul
				  << "last_applied:" << s.last_applied << "\n"
				 //hapus sebelum kumpul
				  << "time to timeout:" << s.time_to_timeout << "\n" ;	

				  
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
			ss << match_index[i] << " ";
		}
		ss << "]";
		return ss.str();
	}
}
