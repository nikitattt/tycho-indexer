use hex_literal::hex;

pub struct FixedPool {
    pub address: [u8; 20],
    pub token0: [u8; 20],
    pub token1: [u8; 20],
}

pub const FIXED_POOLS: [FixedPool; 10] = [
    FixedPool {
        address: hex!("6c561b446416e1a00e8e93e221854d6ea4171372"),
        token0: hex!("4200000000000000000000000000000000000006"),
        token1: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    },
    FixedPool {
        address: hex!("d0b53d9277642d899df5c87a3966a349a798f224"),
        token0: hex!("4200000000000000000000000000000000000006"),
        token1: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    },
    FixedPool {
        address: hex!("fbb6eed8e7aa03b138556eedaf5d271a5e1e43ef"),
        token0: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        token1: hex!("cbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
    },
    FixedPool {
        address: hex!("b4cb800910b228ed3d0834cf79d697127bbb00e5"),
        token0: hex!("4200000000000000000000000000000000000006"),
        token1: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    },
    FixedPool {
        address: hex!("7aea2e8a3843516afa07293a10ac8e49906dabd1"),
        token0: hex!("4200000000000000000000000000000000000006"),
        token1: hex!("cbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
    },
    FixedPool {
        address: hex!("9c087eb773291e50cf6c6a90ef0f4500e349b903"),
        token0: hex!("0b3e328455c4059eeb9e3f84b5543f74e24e7e1b"),
        token1: hex!("4200000000000000000000000000000000000006"),
    },
    FixedPool {
        address: hex!("529d2863a1521d0b57db028168fde2e97120017c"),
        token0: hex!("0b3e328455c4059eeb9e3f84b5543f74e24e7e1b"),
        token1: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    },
    FixedPool {
        address: hex!("aec085e5a5ce8d96a7bdd3eb3a62445d4f6ce703"),
        token0: hex!("22af33fe49fd1fa80c7149773dde5890d3c76f3b"),
        token1: hex!("4200000000000000000000000000000000000006"),
    },
    FixedPool {
        address: hex!("e5b5f522e98b5a2baae212d4da66b865b781db97"),
        token0: hex!("833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        token1: hex!("940181a94a35a4569e4529a3cdfb74e38fd98631"),
    },
    FixedPool {
        address: hex!("3d5d143381916280ff91407febeb52f2b60f33cf"),
        token0: hex!("4200000000000000000000000000000000000006"),
        token1: hex!("940181a94a35a4569e4529a3cdfb74e38fd98631"),
    },
];

pub fn fixed_pool(address: &[u8]) -> Option<&'static FixedPool> {
    FIXED_POOLS
        .iter()
        .find(|pool| pool.address.as_slice() == address)
}

pub fn is_fixed_pool(address: &[u8]) -> bool {
    fixed_pool(address).is_some()
}
