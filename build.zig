const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const version = readVersion(b);
    const options = b.addOptions();
    options.addOption([]const u8, "version", version);

    const mod = b.addModule("zigmq", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    mod.addOptions("build_options", options);

    const exe = b.addExecutable(.{
        .name = "zigmq",
        .root_module = mod,
    });

    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_module = mod,
    });
    const test_cmd = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&test_cmd.step);

    const run_cmd = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);
    if (b.args) |args| run_cmd.addArgs(args);

    const protocol_cmd = b.addSystemCommand(&.{"python3"});
    protocol_cmd.addFileArg(b.path("scripts/protocol_test.py"));
    protocol_cmd.addArg("--exe");
    protocol_cmd.addArtifactArg(exe);

    const protocol_step = b.step("protocol-test", "Run end-to-end protocol regression tests");
    protocol_step.dependOn(&protocol_cmd.step);

    const stress_cmd = b.addSystemCommand(&.{"python3"});
    stress_cmd.addFileArg(b.path("scripts/concurrency_stress.py"));
    stress_cmd.addArg("--quick");
    stress_cmd.addArg("--exe");
    stress_cmd.addArtifactArg(exe);

    const stress_step = b.step("stress-test", "Run concurrent stress regression tests");
    stress_step.dependOn(&stress_cmd.step);

    const bench_cmd = b.addSystemCommand(&.{"python3"});
    bench_cmd.addFileArg(b.path("scripts/concurrency_stress.py"));
    bench_cmd.addArg("--exe");
    bench_cmd.addArtifactArg(exe);

    const bench_step = b.step("benchmark", "Run baseline concurrency throughput harness");
    bench_step.dependOn(&bench_cmd.step);
}

fn readVersion(b: *std.Build) []const u8 {
    const cwd = std.fs.cwd();
    const version = cwd.readFileAlloc(b.allocator, "VERSION", 64) catch @panic("failed to read VERSION");
    return std.mem.trimRight(u8, version, "\r\n");
}
