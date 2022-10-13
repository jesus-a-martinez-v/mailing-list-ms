package grpcapi

import (
	"context"
	"database/sql"
	"google.golang.org/grpc"
	"log"
	"mailing-list-ms/mdb"
	pb "mailing-list-ms/proto"
	"net"
	"time"
)

type MailServer struct {
	pb.UnimplementedMailingListServiceServer
	db *sql.DB
}

func pbEntryToMdbEntry(pbEntry *pb.EmailEntry) mdb.EmailEntry {
	t := time.Unix(pbEntry.ConfirmedAt, 0)
	return mdb.EmailEntry{
		Id:          pbEntry.Id,
		Email:       pbEntry.Email,
		ConfirmedAt: &t,
		OptOut:      pbEntry.OptOut,
	}
}

func mdbEntryToPbEntry(mdbEntry *mdb.EmailEntry) pb.EmailEntry {
	return pb.EmailEntry{
		Id:          mdbEntry.Id,
		Email:       mdbEntry.Email,
		ConfirmedAt: mdbEntry.ConfirmedAt.Unix(),
		OptOut:      mdbEntry.OptOut,
	}

}

func emailResponse(db *sql.DB, email string) (*pb.EmailResponse, error) {
	entry, err := mdb.GetEmail(db, email)

	if err != nil {
		return &pb.EmailResponse{}, err
	}

	if entry == nil {
		return &pb.EmailResponse{}, nil
	}

	res := mdbEntryToPbEntry(entry)

	return &pb.EmailResponse{EmailEntry: &res}, nil
}

func (s *MailServer) CreateEmail(ctx context.Context, in *pb.CreateEmailRequest) (*pb.EmailResponse, error) {
	log.Printf("gRPC CreateEmail: %v\n", in)

	err := mdb.CreateEmail(s.db, in.EmailAddr)
	if err != nil {
		return &pb.EmailResponse{}, err
	}

	return emailResponse(s.db, in.EmailAddr)
}

func (s *MailServer) GetEmail(ctx context.Context, in *pb.GetEmailRequest) (*pb.EmailResponse, error) {
	log.Printf("gRPC GetEmail: %v\n", in)

	return emailResponse(s.db, in.EmailAddr)
}

func (s *MailServer) UpdateEmail(ctx context.Context, in *pb.UpdateEmailRequest) (*pb.EmailResponse, error) {
	log.Printf("gRPC UpdateEmail: %v\n", in)

	entry := pbEntryToMdbEntry(in.EmailEntry)

	err := mdb.UpdateEmail(s.db, entry)
	if err != nil {
		return &pb.EmailResponse{}, err
	}

	return emailResponse(s.db, entry.Email)
}

func (s *MailServer) DeleteEmail(ctx context.Context, in *pb.DeleteEmailRequest) (*pb.EmailResponse, error) {
	log.Printf("gRPC DeleteEmail: %v\n", in)

	err := mdb.DeleteEmail(s.db, in.EmailAddr)
	if err != nil {
		return &pb.EmailResponse{}, err
	}

	return emailResponse(s.db, in.EmailAddr)
}

func (s *MailServer) GetEmailBatch(ctx context.Context, in *pb.GetEmailBatchRequest) (*pb.GetEmailBatchResponse, error) {
	log.Printf("gRPC GetEmailBatch: %v\n", in)

	params := mdb.GetEmailBatchQueryParams{
		Page:  int(in.Page),
		Count: int(in.Count),
	}

	mdbEntries, err := mdb.GetEmailBatch(s.db, params)

	if err != nil {
		return &pb.GetEmailBatchResponse{}, err
	}

	pbEntries := make([]*pb.EmailEntry, 0, len(mdbEntries))

	for i := 0; i < len(mdbEntries); i++ {
		entry := mdbEntryToPbEntry(&mdbEntries[i])
		pbEntries = append(pbEntries, &entry)
	}

	return &pb.GetEmailBatchResponse{
		EmailEntries: pbEntries,
	}, nil
}

func Serve(db *sql.DB, bind string) {
	listener, err := net.Listen("tcp", bind)

	if err != nil {
		log.Fatalf("gRPC server error: failure to bund %v\n", bind)
	}

	grpcServer := grpc.NewServer()
	mailServer := MailServer{db: db}

	pb.RegisterMailingListServiceServer(grpcServer, &mailServer)
	log.Printf("gRPC API server listening on %v\n", bind)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("gRPC server error: %v\n", err)
	}
}
