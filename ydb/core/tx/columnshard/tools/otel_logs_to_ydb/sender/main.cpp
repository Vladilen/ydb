#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/status.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/folder/iterator.h>
#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/fs.h>
#include <util/system/types.h>

#include <algorithm>
#include <iostream>
#include <memory>
#include <string>

namespace {

constexpr TStringBuf ExportMethod = "/opentelemetry.proto.collector.logs.v1.LogsService/Export";

struct TOptions {
    TString Endpoint;
    TString InputDir;
    TString Suffix = ".pb";
    ui64 DeadlineMs = 30000;
    size_t Limit = 0;
    size_t Skip = 0;
    size_t PrintEvery = 100;
    bool ContinueOnError = false;
};

TVector<TString> ListInputFiles(const TString& inputDir, const TString& suffix) {
    if (!TFsPath(inputDir).IsDirectory()) {
        ythrow yexception() << "input path is not a directory: " << inputDir;
    }

    TVector<TString> files;
    TDirIterator::TOptions options;
    options.SetMaxLevel(1);
    options.SetSortByName();
    for (const FTSENT& entry : TDirIterator(inputDir, options)) {
        if (entry.fts_type != FTS_F) {
            continue;
        }
        TString path = entry.fts_path;
        if (!suffix.empty() && !path.EndsWith(suffix)) {
            continue;
        }
        files.push_back(std::move(path));
    }
    Sort(files.begin(), files.end());
    return files;
}

grpc::Status SendOne(
    grpc::GenericStub& stub,
    const TString& path,
    ui64 deadlineMs,
    size_t index,
    size_t total,
    bool printOk)
{
    const TString payload = TFileInput(path).ReadAll();
    grpc::Slice slice(payload.data(), payload.size());
    grpc::ByteBuffer request(&slice, 1);
    grpc::ByteBuffer response;
    grpc::ClientContext context;
    if (deadlineMs > 0) {
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadlineMs));
    }

    grpc::CompletionQueue cq;
    std::unique_ptr<grpc::ClientAsyncResponseReader<grpc::ByteBuffer>> rpc =
        stub.PrepareUnaryCall(&context, TString{ExportMethod}, request, &cq);
    if (!rpc) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "failed to create Export RPC");
    }

    grpc::Status status;
    rpc->StartCall();
    rpc->Finish(&response, &status, reinterpret_cast<void*>(1));

    void* tag = nullptr;
    bool ok = false;
    if (!cq.Next(&tag, &ok) || tag != reinterpret_cast<void*>(1) || !ok) {
        return grpc::Status(grpc::StatusCode::INTERNAL, "Export RPC completion queue failed");
    }

    if (printOk && status.ok()) {
        std::cerr << "sent " << index << "/" << total << " " << path << " bytes=" << payload.size() << std::endl;
    }
    return status;
}

int Run(const TOptions& opts) {
    TVector<TString> files = ListInputFiles(opts.InputDir, opts.Suffix);
    if (opts.Skip >= files.size()) {
        std::cerr << "no files to send after --skip; matched " << files.size() << " files" << std::endl;
        return 0;
    }

    const size_t begin = opts.Skip;
    const size_t requestedEnd = opts.Limit == 0 ? files.size() : Min(files.size(), begin + opts.Limit);
    const size_t total = requestedEnd - begin;

    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(opts.Endpoint, grpc::InsecureChannelCredentials());
    grpc::GenericStub stub(channel);

    size_t sent = 0;
    for (size_t i = begin; i < requestedEnd; ++i) {
        const size_t ordinal = i - begin + 1;
        const bool printOk = opts.PrintEvery != 0 && (ordinal == 1 || ordinal == total || ordinal % opts.PrintEvery == 0);
        const grpc::Status status = SendOne(stub, files[i], opts.DeadlineMs, ordinal, total, printOk);
        if (!status.ok()) {
            std::cerr << "failed " << ordinal << "/" << total << " " << files[i]
                      << " code=" << static_cast<int>(status.error_code())
                      << " message=" << status.error_message() << std::endl;
            if (!opts.ContinueOnError) {
                return 1;
            }
            continue;
        }
        ++sent;
    }

    std::cerr << "done: sent=" << sent << " failed=" << (total - sent) << " total=" << total << std::endl;
    return sent == total ? 0 : 1;
}

} // namespace

int main(int argc, char** argv) {
    TOptions opts;

    NLastGetopt::TOpts cli = NLastGetopt::TOpts::Default();
    cli.AddLongOption('e', "endpoint", "otel_logs_to_ydb gRPC endpoint, for example localhost:4317")
        .Required()
        .RequiredArgument("HOST:PORT")
        .StoreResult(&opts.Endpoint);
    cli.AddLongOption('i', "input-dir", "directory with serialized ExportLogsServiceRequest *.pb files")
        .Required()
        .RequiredArgument("PATH")
        .StoreResult(&opts.InputDir);
    cli.AddLongOption("suffix", "input file suffix filter; empty string means all regular files")
        .Optional()
        .RequiredArgument("SUFFIX")
        .StoreResult(&opts.Suffix);
    cli.AddLongOption("deadline-ms", "per-request deadline in milliseconds; 0 disables the deadline")
        .Optional()
        .RequiredArgument("MS")
        .StoreResult(&opts.DeadlineMs);
    cli.AddLongOption("skip", "number of matched files to skip before sending")
        .Optional()
        .RequiredArgument("N")
        .StoreResult(&opts.Skip);
    cli.AddLongOption("limit", "maximum number of files to send; 0 means no limit")
        .Optional()
        .RequiredArgument("N")
        .StoreResult(&opts.Limit);
    cli.AddLongOption("print-every", "print progress every N successful requests; 0 disables progress")
        .Optional()
        .RequiredArgument("N")
        .StoreResult(&opts.PrintEvery);
    cli.AddLongOption("continue-on-error", "continue sending after failed requests")
        .Optional()
        .NoArgument()
        .SetFlag(&opts.ContinueOnError);

    try {
        NLastGetopt::TOptsParseResult parseResult(&cli, argc, argv);
        return Run(opts);
    } catch (const std::exception& ex) {
        std::cerr << "fatal: " << ex.what() << std::endl;
        return 1;
    }
}
