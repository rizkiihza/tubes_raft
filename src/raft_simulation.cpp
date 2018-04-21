#include <raft_simulation.hpp>

namespace raft {
	RaftSimulation::RaftSimulation(int cluster_size_){
		cluster_size = cluster_size_;
		servers.resize(cluster_size+1);
		server_online.resize(cluster_size+1);
		for( int i = 1; i <= cluster_size; ++ i ){
			servers[i] = Server( cluster_size, i, Sender(this,i) );
			server_online[i] = true;
		}
		
		for( int i = 1; i <= cluster_size; ++ i ){
			for( int k = 1; k <= cluster_size; ++ k ){
				Connect(i,k);
			}
		}

		mailbox_index = 0;
	}

	void RaftSimulation::Connect(int a, int b){
		connection_matrix[ std::make_pair(a,b) ] = true;
		connection_matrix[ std::make_pair(b,a) ] = true;
	}

	void RaftSimulation::Disconnect(int a, int b){
		connection_matrix[ std::make_pair(a,b) ] = false;
		connection_matrix[ std::make_pair(b,a) ] = false;
	}

	void RaftSimulation::Crash(int a){
		server_online[a] = false;
		servers[a].Crash();
	}

	void RaftSimulation::Start(int server_index){
		server_online[server_index] = true;
	}

	void RaftSimulation::SetTimeout(int a, int ttt){
		servers[a].SetTimeout(ttt);
	}

	void RaftSimulation::Timestep(){
		for( int i = 1; i <= cluster_size; ++ i ){
			if( !server_online[i] ) continue;
			servers[i].Timestep();
		}
	}


	void RaftSimulation::Send(int sender_id, int to, RequestVoteRPC payload){
		Mail mail;
		mail.from_id = sender_id;
		mail.to_id = to;
		mail.type = MailType::RVRPC;
		mail.rvrpc = payload;

		mailbox.push_back( mail );
	}

	void RaftSimulation::Send(int sender_id, int to, AppendEntriesRPC payload){
		Mail mail;
		mail.from_id = sender_id;
		mail.to_id = to;
		mail.type = MailType::AERPC;
		mail.aerpc = payload;

		mailbox.push_back( mail );
	}

	void RaftSimulation::Send(int sender_id, int to, RequestVoteReply payload){
		Mail mail;
		mail.from_id = sender_id;
		mail.to_id = to;
		mail.type = MailType::RVREPLY;
		mail.rvreply = payload;

		mailbox.push_back( mail );
	}

	void RaftSimulation::Send(int sender_id, int to, AppendEntriesReply payload){
		Mail mail;
		mail.from_id = sender_id;
		mail.to_id = to;
		mail.type = MailType::AEREPLY;
		mail.aereply = payload;

		mailbox.push_back( mail );
	}

	void RaftSimulation::FlushOutbox(){
		int limit = mailbox.size();
		for( int i = mailbox_index; i < limit; ++ i ){
			Mail mail = mailbox[i];
			if( !connection_matrix[ std::make_pair(mail.from_id, mail.to_id) ] ) continue;
			if( !server_online[mail.to_id] ) continue;

			if( mail.type == MailType::RVRPC ){
				servers[mail.to_id].Receive(mail.rvrpc);
			} else if( mail.type == MailType::RVREPLY ){
				servers[mail.to_id].Receive(mail.rvreply);
			} else if( mail.type == MailType::AERPC ){
				servers[mail.to_id].Receive(mail.aerpc);
			} else if( mail.type == MailType::AEREPLY ){
				servers[mail.to_id].Receive(mail.aereply);
			}
		}

		mailbox_index = limit;
	}

	void RaftSimulation::StoreOutbox(std::string k){
		int limit = mailbox.size();
		for( int i = mailbox_index; i < limit; ++ i ){
			stored_mailbox[k].push_back(mailbox[i]);
		}
	}

	void RaftSimulation::DeleteOutbox(){
		mailbox.resize(mailbox_index);
	}

	void RaftSimulation::PerfectShuffleOutbox(){
		int outbox_size = mailbox.size() - mailbox_index;
		std::vector<Mail> tmp_mailbox(outbox_size);

		for( int i = 0; i < outbox_size/2; ++ i ){
			tmp_mailbox[i*2] = mailbox[mailbox_index+i];
		}

		for( int i = outbox_size/2, k = 0; i < outbox_size; ++ i, ++ k ){
			tmp_mailbox[k*2+1] = mailbox[mailbox_index+i];
		}

		for( int i = 0; i < outbox_size; ++ i ){
			mailbox[mailbox_index+i] = tmp_mailbox[i];
		}
	}

	void RaftSimulation::RetrieveOutbox(std::string k){
		std::vector<Mail>& tmp_mailbox = stored_mailbox[k];
		
		for( int i = 0; i < tmp_mailbox.size(); ++ i ){
			mailbox.push_back(tmp_mailbox[i]);
		}
	}

	void RaftSimulation::ClientSendRequest(int a, Log log){
		servers[a].Receive(log);
	}


	void RaftSimulation::PrintServerState(int i){
		std::cout << servers[i] << std::endl;
	}

	void RaftSimulation::PrintServerLeaderState(int i){
		std::cout << servers[i].GetLeaderStateString() << std::endl;
	}

	void RaftSimulation::PrintOutbox(){
		std::cout << "<<Outbox:" << mailbox.size() - mailbox_index << ">>" << std::endl;
		for( int i = mailbox_index; i < mailbox.size(); ++ i ){
			std::cout << mailbox[i] << std::endl;
		}
	}

	void RaftSimulation::PrintServerRole(int i){
		std::cout << servers[i].GetRoleString() << std::endl;
	}

	void RaftSimulation::PrintServerLog(int i){
		std::cout << servers[i].GetLogString() << std::endl;
	}

	void RaftSimulation::PrintServerData(int i){
		std::cout << servers[i].GetData() << std::endl;		
	}

	void RaftSimulation::PrintServerCommitIndex(int i){
		std::cout << servers[i].GetCommitIndex() << std::endl;
	}
}