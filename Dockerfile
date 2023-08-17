FROM public.ecr.aws/r5b3e0r5/3box/rust-builder:latest as builder

RUN mkdir -p /home/builder/rust-ceramic-migration-tests
WORKDIR /home/builder/rust-ceramic-migration-tests

# Use the same ids as the parent docker image by default
ARG UID=1001
ARG GID=1001

# Define the type of build to make. One of release or debug.
ARG BUILD_MODE=release

# Copy in source code
COPY . .

# Build application using a docker cache
# To clear the cache use:
#   docker builder prune --filter type=exec.cachemount
RUN --mount=type=cache,target=/home/builder/.cargo,uid=$UID,gid=$GID \
	--mount=type=cache,target=/home/builder/rust-ceramic-migration-tests/target,uid=$UID,gid=$GID \
    make $BUILD_MODE && \
    cp ./target/release/rust-ceramic-migration-tests ./

FROM ubuntu:latest as tester

COPY --from=builder /home/builder/rust-ceramic-migration-tests/rust-ceramic-migration-tests /usr/bin

ENV RUST_BACKTRACE=1
ENV CERAMIC_URLS=""

ENTRYPOINT ["/usr/bin/rust-ceramic-migration-tests"]
