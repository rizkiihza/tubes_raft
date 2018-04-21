#include <mail.hpp>
#include <sstream>

namespace raft {
	std::ostream & operator<<(std::ostream &os, const Mail& m){
		std::string type_str, payload;

		if( m.type == MailType::RVRPC ){
			type_str = "RequestVoteRPC";
			std::stringstream s;
			s << "term:" << m.rvrpc.term  << "\ncandidate_id:"  << m.rvrpc.candidate_id <<  "\nlast_log_index:" << m.rvrpc.last_log_index << "\nlast_log_term:" << m.rvrpc.last_log_term;
			payload = s.str();
		} else if( m.type == MailType::RVREPLY ){
			type_str = "RequestVoteReply";
			std::stringstream s;
			s << "from_id:" << m.rvreply.from_id << "\nvote_granted:" << (m.rvreply.vote_granted?"TRUE":"FALSE");
			payload = s.str();
		} else if( m.type == MailType::AERPC ){
			type_str = "AppendEntriesRPC";
			std::stringstream log_str;
			if( m.aerpc.logs.size() == 1 ){
				Log log = m.aerpc.logs[0];

				if( log.operation == Operation::MULTIPLY ){
					log_str << "*" << log.payload;
				} else if( log.operation == Operation::ADD ){
					log_str << "+" << log.payload;
				} else if( log.operation == Operation::SUBTRACT ){
					log_str << "-" << log.payload;
				} else if( log.operation == Operation::REPLACE ){
					log_str << "||" << log.payload;
				}
			} else {
				log_str << "BLANK";
			}

			std::stringstream s;
			s << "term:" << m.aerpc.term << "\nleader_id:" << m.aerpc.leader_id << "\nprev_log_index:" << m.aerpc.prev_log_index << "\nprev_log_term:" << m.aerpc.prev_log_term << "\nlogs:" << log_str.str() << "\nleader_commit_index:" << m.aerpc.leader_commit_index;
			payload = s.str();
		} else if( m.type == MailType::AEREPLY ){
			type_str = "AppendEntriesReply";

			std::stringstream s;
			s << "from_id:" << m.aereply.from_id << "\nsuccess:" << (m.aereply.success?"true":"false");
			payload = s.str();
		}

		return os << "<<Mail>>\n" << "to:" << m.to_id << "\n" 
		  << "from:" << m.from_id << "\n"
		  << "type:" << type_str << "\n"
		  << "payload:\n" << payload << "\n";

	}

}