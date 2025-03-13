// components/ServiceWrapper.tsx
import React, { FC } from 'react';
import { useSession } from 'next-auth/react';
import { 
  Box, 
  VStack, 
  Heading, 
  Button, 
  Text 
} from '@chakra-ui/react';
import { BlockchainService } from '@/interfaces/ServiceInterface';
import Layout from './Layout';

interface ServiceWrapperProps {
  service: BlockchainService;
}

const ServiceWrapper: FC<ServiceWrapperProps> = ({ service }) => {
  const { data: session } = useSession();
  const { address } = useEthersStore((state) => ({ 
    address: state.address 
  }));

  if (!session && service.requiresAuthentication) {
    return (
      <Layout title={service.title}>
        <VStack spacing={4} align="center" justify="center" minH="50vh">
          <Heading>Authentication Required</Heading>
          <Text>{service.description}</Text>
          <Button onClick={() => signIn()}>Sign In</Button>
        </VStack>
      </Layout>
    );
  }

  return (
    <Layout title={service.title} address={address}>
      <VStack spacing={4} w="full" maxW="container.xl" mx="auto" p={4}>
        <Heading>{service.title}</Heading>
        <Text>{service.description}</Text>
        
        {service.actions.map((action) => (
          <Button 
            key={action.name}
            onClick={action.execute}
            isDisabled={
              service.requiresAuthentication && !session
            }
          >
            {action.name}
          </Button>
        ))}
      </VStack>
    </Layout>
  );
};

export default ServiceWrapper;