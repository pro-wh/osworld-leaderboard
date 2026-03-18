"""Aggregate results.json and provenance.json files from sharded evaluation runs."""

import argparse
import json
import os
import sys


def collect_github_actions_metadata() -> dict | None:
    """Collect GitHub Actions run metadata when available."""
    if not os.environ.get("GITHUB_ACTIONS"):
        return None

    env = os.environ
    repository = env.get("GITHUB_REPOSITORY")
    server_url = env.get("GITHUB_SERVER_URL")
    api_url = env.get("GITHUB_API_URL")
    run_id = env.get("GITHUB_RUN_ID")
    run_url = None
    repository_url = None
    if repository and server_url and run_id:
        run_url = f"{server_url}/{repository}/actions/runs/{run_id}"
    if repository and server_url:
        repository_url = f"{server_url}/{repository}"
    run_logs_url = None
    if repository and api_url and run_id:
        run_logs_url = f"{api_url}/repos/{repository}/actions/runs/{run_id}/logs"

    metadata = {
        "run_url": run_url,
        "run_logs_url": run_logs_url,
        "ref": env.get("GITHUB_REF"),
        "sha": env.get("GITHUB_SHA"),
        "repository_url": repository_url,
        "workflow_ref": env.get("GITHUB_WORKFLOW_REF"),
        "workflow_sha": env.get("GITHUB_WORKFLOW_SHA"),
    }

    return {key: value for key, value in metadata.items() if value}


def main():
    parser = argparse.ArgumentParser(description="Aggregate shard results and provenance into single output files")
    parser.add_argument("--num-shards", type=int, required=True)
    parser.add_argument("--shard-results-dir", default="shard-results")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()

    overall_sum = 0.0
    overall_count = 0
    per_domain: dict = {}
    participants = {}

    local_github_actions = collect_github_actions_metadata()
    image_digests = None
    shard_timestamps = []
    expected_dirs = [f"shard-{i}" for i in range(args.num_shards)]
    actual_dirs = os.listdir(args.shard_results_dir)
    if set(actual_dirs) != set(expected_dirs):
        print(f"Error: expected shard dirs {sorted(expected_dirs)}, got {sorted(actual_dirs)}")
        sys.exit(1)

    for shard_dir in expected_dirs:
        base = os.path.join(args.shard_results_dir, shard_dir)

        # Aggregate results
        with open(os.path.join(base, "results.json")) as f:
            data = json.load(f)
        participants = data.get("participants", participants)
        for result in data.get("results", []):
            o = result["overall"]
            overall_sum += o["sum"]
            overall_count += o["count"]
            for domain, d in result["per_domain"].items():
                if domain not in per_domain:
                    per_domain[domain] = {"sum": 0.0, "count": 0}
                per_domain[domain]["sum"] += d["sum"]
                per_domain[domain]["count"] += d["count"]

        # Merge provenance
        with open(os.path.join(base, "provenance.json")) as f:
            prov = json.load(f)

        shard_timestamps.append(prov.get("timestamp"))

        if image_digests is None:
            image_digests = prov.get("image_digests")
            if not image_digests:
                print(f"Error: missing image_digests in {shard_dir}")
                sys.exit(1)
        elif prov.get("image_digests") != image_digests:
            print(f"Error: image_digests mismatch in {shard_dir}")
            print(f"  Expected: {image_digests}")
            print(f"  Got:      {prov.get('image_digests')}")
            sys.exit(1)

        shard_github_actions = prov.get("github_actions")
        if shard_github_actions != local_github_actions:
            print(f"Error: github_actions mismatch in {shard_dir}")
            print(f"  Expected: {local_github_actions}")
            print(f"  Got:      {shard_github_actions}")
            sys.exit(1)

    success_rate = overall_sum / overall_count if overall_count else 0.0

    os.makedirs(args.output_dir, exist_ok=True)

    with open(os.path.join(args.output_dir, "results.json"), "w") as f:
        json.dump({
            "participants": participants,
            "results": [{
                "overall": {"sum": overall_sum, "count": overall_count},
                "success_rate": success_rate,
                "per_domain": per_domain,
            }],
        }, f, indent=2)
    print(f"success_rate={success_rate:.4f} over {overall_count} examples")

    provenance: dict = {
        "image_digests": image_digests,
        "shard_timestamps": shard_timestamps,
    }
    if local_github_actions:
        provenance["github_actions"] = local_github_actions
    with open(os.path.join(args.output_dir, "provenance.json"), "w") as f:
        json.dump(provenance, f, indent=2)
    print(f"Recorded provenance ({args.num_shards} shards)")


if __name__ == "__main__":
    main()
