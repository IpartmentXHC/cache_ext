#!/bin/bash
set -eu -o pipefail

# Install Linux build dependencies
echo "Installing dependencies..."
sudo apt-get update
sudo apt-get install -y build-essential bc bison flex rsync libelf-dev \
			libssl-dev libncurses-dev dwarves clang lld \
			llvm python3 python3-pip

# Kernel build.py script dependencies
pip3 install yanniszark_common

SCRIPT_PATH=$(realpath $0)
BASE_DIR=$(dirname $SCRIPT_PATH)
LINUX_PATH="$BASE_DIR/linux"

cd "$LINUX_PATH"
if [[ ! -e "Makefile" ]]; then
    git submodule update --init --recursive
fi

# Clean previous builds
make distclean

echo "Configuring kernel..."
make olddefconfig

# Ignore 'yes' exit status
{ yes '' || true;} | make localmodconfig

scripts/config --set-str LOCALVERSION "-cache-ext"
scripts/config --set-str SYSTEM_TRUSTED_KEYS ''
scripts/config --set-str SYSTEM_REVOCATION_KEYS ''
scripts/config --enable CONFIG_BPF_SYSCALL
scripts/config --enable CONFIG_DEBUG_INFO_BTF

echo "Building and installing the kernel..."
echo "If prompted, hit enter to continue."
python3 build.py install --enable-mglru

echo "Building and installing libbpf..."
# Default location:
#	Library: /usr/local/lib64/libbpf.{a,so}
#	Headers: /usr/local/include/bpf
make -C tools/lib/bpf -j
sudo make -C tools/lib/bpf install

# Add ld.so.conf.d entry for libbpf
if [[ ! -e /etc/ld.so.conf.d/libbpf.conf ]]; then
	echo "/usr/local/lib64" | sudo tee /etc/ld.so.conf.d/libbpf.conf > /dev/null
	sudo ldconfig
	echo "Added /usr/local/lib64 to /etc/ld.so.conf.d/libbpf.conf"
else
	echo "/usr/local/lib64 already exists in /etc/ld.so.conf.d/libbpf.conf"
fi

echo "Building and install bpftool..."
make -C tools/bpf/bpftool -j
# Default location:
#	Binary: /usr/local/sbin/bpftool (version v7.3.0)
sudo make -C tools/bpf/bpftool install

installed_kernel=""
if compgen -G "/boot/vmlinuz-*-cache-ext+" > /dev/null; then
	installed_kernel=$(basename "$(ls -1 /boot/vmlinuz-*-cache-ext+ | sort | tail -n 1)")
	installed_kernel=${installed_kernel#vmlinuz-}
else
	echo "Cannot find cache_ext kernel image in /boot (expected: /boot/vmlinuz-*-cache-ext+)."
	echo "Please verify kernel installation manually."
	exit 1
fi

echo "cache_ext kernel installed successfully: ${installed_kernel}"

if [[ -f /boot/grub/grub.cfg ]]; then
	if awk -F\' '/menuentry / {print $2}' /boot/grub/grub.cfg | grep -qm 1 "${installed_kernel}"; then
		echo "Detected GRUB bootloader. To boot into cache_ext kernel, run:"
		echo -e "    sudo grub-reboot \"Advanced options for Ubuntu>Ubuntu, with Linux ${installed_kernel}\""
		echo -e "    sudo reboot now"
	else
		echo "GRUB config exists but does not contain kernel ${installed_kernel}."
		echo "Run 'sudo update-grub' and select the cache_ext kernel on next boot."
	fi
elif [[ -f /boot/extlinux/extlinux.conf ]]; then
	echo "Detected EXTLINUX bootloader. Please set ${installed_kernel} as default in /boot/extlinux/extlinux.conf and reboot."
elif [[ -f /boot/boot.scr ]]; then
	echo "Detected U-Boot boot script (/boot/boot.scr). Please update boot script/default kernel to ${installed_kernel} and reboot."
else
	echo "Bootloader config not auto-detected."
	echo "Kernel is installed; please choose ${installed_kernel} as the default kernel in your bootloader and reboot."
fi
