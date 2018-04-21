#include <sender.hpp>

namespace raft {
	Sender::Sender(){
		simulation = NULL;
		sender_id = 0;
	}

	Sender::Sender(Mailer* s, int id){
		simulation = s;
		sender_id = id;
	}

	void Sender::Send(int to, RequestVoteRPC payload){
		simulation->Send(sender_id, to, payload);
	}

	void Sender::Send(int to, AppendEntriesRPC payload){
		simulation->Send(sender_id, to, payload);
	}

	void Sender::Send(int to, RequestVoteReply payload){
		simulation->Send(sender_id, to, payload);
	}
	
	void Sender::Send(int to, AppendEntriesReply payload){
		simulation->Send(sender_id, to, payload);
	}

}